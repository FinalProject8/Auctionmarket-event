package org.example.auctionmarketevent.springbatch;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.example.auctionmarketevent.springbatch.job.dto.AuctionsWinningBidDto;
import org.example.auctionmarketevent.springbatch.job.writer.BigQueryItemWriter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.test.MetaDataInstanceFactory;

import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;

@ExtendWith(MockitoExtension.class)
class BigQueryItemWriterTest {

	private static final Logger log = LoggerFactory.getLogger(BigQueryItemWriterTest.class);

	@Mock
	private Storage mockStorage;

	@Spy
	private CsvMapper csvMapper = new CsvMapper();

	@InjectMocks
	private BigQueryItemWriter writer;

	private final String testBucketName = "test-bucket";
	private static final String GCS_FILE_URIS_KEY = "gcsFileUris";
	private static final String MAX_TIMESTAMP_KEY = "maxProcessedTimestampInChunk";

	// 테스트 내에서 StepExecution 을 관리하기 위한 멤버 변수
	private StepExecution stepExecution;

	@BeforeEach
	void setUp() {
		writer = new BigQueryItemWriter(mockStorage, testBucketName);
		stepExecution = MetaDataInstanceFactory.createStepExecution();
		stepExecution.getExecutionContext().put(GCS_FILE_URIS_KEY, new ArrayList<String>()); // URI 리스트 초기화
		writer.saveStepExecution(stepExecution);
	}

	@Test
	void 데이터_쓰기_및_GCS_업로드_성공() throws Exception {
		// given
		Instant now = Instant.now();
		Timestamp nowTimestamp = Timestamp.from(now);
		List<AuctionsWinningBidDto> items = List.of(
			AuctionsWinningBidDto.builder()
				.auctionId(1L).productId(101L).productName("테스트 상품 1")
				.productCategory("테스트 카테고리 1").maxPrice(100L)
				.auctionStartTime(now).auctionEndTime(now).lastModified(now)
				.build(),
			AuctionsWinningBidDto.builder()
				.auctionId(2L).productId(102L).productName("테스트 상품 2")
				.productCategory("테스트 카테고리 2").maxPrice(200L)
				.auctionStartTime(now).auctionEndTime(now).lastModified(now.minusSeconds(60))
				.build()
		);
		Chunk<AuctionsWinningBidDto> chunk = new Chunk<>(items);

		when(mockStorage.create(any(BlobInfo.class), any(byte[].class))).thenReturn(null);

		// when
		writer.write(chunk);

		// then
		// mockStorage create 메서드가 1번 호출되었는지 확인 + 호출된 값 확인
		ArgumentCaptor<BlobInfo> blobInfoCaptor = ArgumentCaptor.forClass(BlobInfo.class); // BlobInfo 캡처
		ArgumentCaptor<byte[]> csvDataCaptor = ArgumentCaptor.forClass(byte[].class); // byte[] 캡처
		verify(mockStorage, times(1)).create(blobInfoCaptor.capture(), csvDataCaptor.capture());

		// BlobInfo 검증
		BlobInfo capturedBlobInfo = blobInfoCaptor.getValue();

		assertEquals(testBucketName, capturedBlobInfo.getBlobId().getBucket());
		assertTrue(capturedBlobInfo.getBlobId().getName().startsWith("batch_load_"));
		assertTrue(capturedBlobInfo.getBlobId().getName().endsWith(".csv"));
		assertEquals("text/csv", capturedBlobInfo.getContentType());

		// CSV 데이터 내용 검증
		String csvContent = new String(csvDataCaptor.getValue(), StandardCharsets.UTF_8);
		log.info(csvContent);
		assertTrue(csvContent.contains("1,101,\"테스트 상품 1\",\"테스트 카테고리 1\",100,"));
		assertTrue(csvContent.contains("2,102,\"테스트 상품 2\",\"테스트 카테고리 2\",200,"));
		assertFalse(csvContent.contains("lastModified")); // @JsonIgnore 확인

		// ExecutionContext 에 GCS URI 추가되었는지 검증
		ExecutionContext executionContext = stepExecution.getExecutionContext();
		List<String> gcsUris = (List<String>)executionContext.get(GCS_FILE_URIS_KEY);
		assertNotNull(gcsUris);
		assertEquals(1, gcsUris.size());
		assertTrue(gcsUris.get(0).startsWith("gs://" + testBucketName + "/batch_load_"));

		// ExecutionContext 에 최신 타임스탬프가 저장되었는지 검증
		Timestamp maxTimestamp = (Timestamp)executionContext.get(MAX_TIMESTAMP_KEY);
		assertNotNull(maxTimestamp);
		assertEquals(nowTimestamp.toInstant(), maxTimestamp.toInstant());
	}

	// @Test
	// void 빈_Chunk_쓰기() throws Exception {
	// 	// given
	// 	Chunk<AuctionsWinningBidDto> emptyChunk = new Chunk<>(List.of());
	//
	// 	// when
	// 	writer.write(emptyChunk);
	//
	// 	// then
	// 	verify(mockStorage, never()).create(any(BlobInfo.class), any(byte[].class));
	//
	// 	// ExecutionContext 에 변화가 없는지 확인
	// 	ExecutionContext executionContext = stepExecution.getExecutionContext();
	// 	List<String> gcsUris = (List<String>) executionContext.get(GCS_FILE_URIS_KEY);
	// 	assertTrue(gcsUris == null || gcsUris.isEmpty());
	// 	assertNull(executionContext.get(MAX_TIMESTAMP_KEY));
	// }
}

