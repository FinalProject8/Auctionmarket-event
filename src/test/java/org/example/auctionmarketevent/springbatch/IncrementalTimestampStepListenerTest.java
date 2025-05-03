package org.example.auctionmarketevent.springbatch;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

import org.example.auctionmarketevent.common.listener.IncrementalTimestampStepListener;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.util.ReflectionTestUtils;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.JobStatistics;
import com.google.cloud.bigquery.JobStatus;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;

@ExtendWith(MockitoExtension.class)
class IncrementalTimestampStepListenerTest {

	@Mock
	private JdbcTemplate mockJdbcTemplate;
	@Mock
	private BigQuery mockBigQuery;
	@Mock
	private Storage mockStorage;
	@Mock
	private StepExecution mockStepExecution;
	@Mock
	private JobExecution mockJobExecution;
	@Mock
	private ExecutionContext mockExecutionContext;
	@Mock
	private Job mockBigQueryJob;
	@Mock
	private JobStatus mockJobStatus;
	@Mock
	private JobStatistics.LoadStatistics mockLoadStats;

	@InjectMocks
	private IncrementalTimestampStepListener listener;

	@Captor
	private ArgumentCaptor<Timestamp> timestampCaptor;
	@Captor
	private ArgumentCaptor<String> sqlCaptor;
	@Captor
	private ArgumentCaptor<Object[]> paramsCaptor;
	@Captor
	private ArgumentCaptor<JobInfo> jobInfoCaptor;
	@Captor
	private ArgumentCaptor<List<BlobId>> blobIdListCaptor;

	private final String JOB_NAME = "testJob";
	private final String DATASET_NAME = "test_dataset";
	private final String TABLE_NAME = "test_table";
	private final String BUCKET_NAME = "test-bucket";

	@BeforeEach
	void setUp() {
		ReflectionTestUtils.setField(listener, "jobName", JOB_NAME);
		ReflectionTestUtils.setField(listener, "datasetName", DATASET_NAME);
		ReflectionTestUtils.setField(listener, "tableName", TABLE_NAME);
		ReflectionTestUtils.setField(listener, "gcsBucketName", BUCKET_NAME);

		when(mockStepExecution.getExecutionContext()).thenReturn(mockExecutionContext);
	}

	// beforeStep 테스트
	@Test
	void beforeStep_기존_타임스탬프_존재_시_Context_에_저장() {
		// given
		Timestamp expectedTimestamp = Timestamp.valueOf(LocalDateTime.of(2025, 4, 28, 0, 0, 0));
		when(mockJdbcTemplate.queryForObject(anyString(), eq(Timestamp.class), eq(JOB_NAME)))
			.thenReturn(expectedTimestamp);

		// when
		listener.beforeStep(mockStepExecution);

		// then
		verify(mockExecutionContext).put("lastProcessedTimestamp", expectedTimestamp);
		verify(mockJdbcTemplate).queryForObject(
			"SELECT last_processed_timestamp FROM batch_job_metadata WHERE job_name = ?",
			Timestamp.class,
			JOB_NAME);
	}

	@Test
	void beforeStep_기존_타임스탬프가_없을_시_MIN_타임스탬프_저장() {
		// given
		when(mockJdbcTemplate.queryForObject(anyString(), eq(Timestamp.class), eq(JOB_NAME)))
			.thenThrow(new EmptyResultDataAccessException(1));

		// when
		listener.beforeStep(mockStepExecution);

		// then
		verify(mockExecutionContext).put("lastProcessedTimestamp", Timestamp.valueOf(LocalDateTime.MIN));
	}

	@Test
	void beforeStep_DB_조회_중_예외_발생_시_MIN_타임스탬프_저장() {
		// given
		when(mockJdbcTemplate.queryForObject(anyString(), eq(Timestamp.class), eq(JOB_NAME)))
			.thenThrow(new RuntimeException("DB 연결 오류"));

		// when
		listener.beforeStep(mockStepExecution);

		// then
		verify(mockExecutionContext).put("lastProcessedTimestamp", Timestamp.valueOf(LocalDateTime.MIN));
	}

	@Test
	void afterStep_성공_및_GCS_파일_없을_시_작업_종료() {
		// given
		when(mockStepExecution.getExitStatus()).thenReturn(ExitStatus.COMPLETED);
		when(mockExecutionContext.get("gcsFileUris")).thenReturn(new ArrayList<String>());

		// when
		ExitStatus exitStatus = listener.afterStep(mockStepExecution);

		// then
		verify(mockBigQuery, never()).create(any(JobInfo.class));
		verify(mockJdbcTemplate, never()).update(anyString(), any(), anyString());
		verify(mockStorage, never()).delete(anyList());
		assertEquals(ExitStatus.COMPLETED, exitStatus);
	}

	@Test
	void afterStep_Step_실패_시_작업_종료() {
		// given
		when(mockStepExecution.getExitStatus()).thenReturn(ExitStatus.FAILED);
		when(mockExecutionContext.get("gcsFileUris")).thenReturn(List.of("gs://test-bucket/file1.csv"));

		// when
		ExitStatus exitStatus = listener.afterStep(mockStepExecution);

		// then
		verify(mockBigQuery, never()).create(any(JobInfo.class));
		verify(mockJdbcTemplate, never()).update(anyString(), any(), anyString());
		verify(mockStorage, never()).delete(anyList());
		assertEquals(ExitStatus.FAILED, exitStatus);
	}

	@Test
	void afterStep_BigQuery_로드_실패_시_ExitStatus_FAILED_로_변경() throws InterruptedException {
		// given
		when(mockStepExecution.getExitStatus()).thenReturn(ExitStatus.COMPLETED);
		List<String> gcsUris = List.of("gs://test-bucket/file1.csv");
		when(mockExecutionContext.get("gcsFileUris")).thenReturn(gcsUris);

		// BigQuery 로드 실패 설정
		setupBigQueryLoadFailure("BigQuery 로드 테스트 오류");

		// when
		ExitStatus exitStatus = listener.afterStep(mockStepExecution);

		// then
		// BigQuery 로드 시도 확인
		verify(mockBigQuery).create(any(JobInfo.class));
		verify(mockBigQueryJob).waitFor();

		// 메타데이터 업데이트, GCS 삭제 미호출 확인
		verify(mockJdbcTemplate, never()).update(anyString(), any(), anyString());
		verify(mockStorage, never()).delete(anyList());

		// StepExecution 의 ExitStatus 가 FAILED 로 설정되었는지 확인
		verify(mockStepExecution).setExitStatus(ExitStatus.FAILED);
	}

	@Test
	void afterStep_BigQuery_로드_중_InterruptedException_발생_시_FAILED_로_변경() throws InterruptedException {
		// given
		when(mockStepExecution.getExitStatus()).thenReturn(ExitStatus.COMPLETED);
		List<String> gcsUris = List.of("gs://test-bucket/file1.csv");
		when(mockExecutionContext.get("gcsFileUris")).thenReturn(gcsUris);

		// BigQuery 로드 시 InterruptedException 발생 설정
		when(mockBigQuery.create(any(JobInfo.class))).thenReturn(mockBigQueryJob);
		when(mockBigQueryJob.waitFor()).thenThrow(new InterruptedException("BigQuery 대기 중 인터럽트"));

		// when
		ExitStatus exitStatus = listener.afterStep(mockStepExecution);

		// then:
		// BigQuery 로드 시도 확인
		verify(mockBigQuery).create(any(JobInfo.class));
		verify(mockBigQueryJob).waitFor();

		// 메타데이터 업데이트, GCS 삭제 미호출 확인
		verify(mockJdbcTemplate, never()).update(anyString(), any(), anyString());
		verify(mockStorage, never()).delete(anyList());

		// StepExecution 의 ExitStatus 가 FAILED 로 설정되었는지 확인
		verify(mockStepExecution).setExitStatus(ExitStatus.FAILED);

		// 스레드 인터럽트 상태 복원 확인
		assertTrue(Thread.currentThread().isInterrupted());
		// 테스트 후 스레드 상태 초기화
		Thread.interrupted();
	}

	// 헬퍼 메서드
	private void setupBigQueryLoadSuccess() throws InterruptedException {
		when(mockBigQuery.create(any(JobInfo.class))).thenReturn(mockBigQueryJob);
		when(mockBigQueryJob.waitFor()).thenReturn(mockBigQueryJob); // 완료된 Job 반환
		when(mockBigQueryJob.getStatus()).thenReturn(mockJobStatus);
		when(mockJobStatus.getError()).thenReturn(null); // 에러 없음 => 성공
		when(mockBigQueryJob.getStatistics()).thenReturn(mockLoadStats);
		when(mockLoadStats.getOutputRows()).thenReturn(100L);
		when(mockBigQueryJob.getJobId()).thenReturn(JobId.of("test-project", "test-job-id"));
	}

	private void setupBigQueryLoadFailure(String errorMessage) throws InterruptedException {
		when(mockBigQuery.create(any(JobInfo.class))).thenReturn(mockBigQueryJob);
		when(mockBigQueryJob.waitFor()).thenReturn(mockBigQueryJob); // 완료된 Job 반환 (실패 상태)
		when(mockBigQueryJob.getStatus()).thenReturn(mockJobStatus);
		BigQueryError error = new BigQueryError("reason", "location", errorMessage);
		when(mockJobStatus.getError()).thenReturn(error); // 에러 객체 반환 => 실패
		when(mockBigQueryJob.getJobId()).thenReturn(JobId.of("test-project", "test-job-id"));
	}
}

