package org.example.auctionmarketevent.common.scheduler;

import java.time.LocalDateTime;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class BatchJobScheduler {

	private final JobLauncher jobLauncher;
	private final Job mysqlToBigQueryJob;

	// 생성자
	@Autowired
	public BatchJobScheduler(JobLauncher jobLauncher,
		@Qualifier("mysqlToBigQueryJob") Job mysqlToBigQueryJob) {
		this.jobLauncher = jobLauncher;
		this.mysqlToBigQueryJob = mysqlToBigQueryJob;
	}

	// 스케줄링 설정
	@Scheduled(cron = "0 0 0 * * ?")
	public void runMysqlToBigQueryJob() {

		try {
			JobParameters jobParameters = new JobParametersBuilder()
				.addLocalDateTime("scheduledTime", LocalDateTime.now()) // 현재 시간을 파라미터로 추가
				.toJobParameters();

			// Job 실행
			jobLauncher.run(mysqlToBigQueryJob, jobParameters);

		} catch (JobExecutionAlreadyRunningException | JobRestartException | JobInstanceAlreadyCompleteException |
				 JobParametersInvalidException e) {
			log.error("스케줄러 실패", e);
		} catch (Exception e) {
			log.error("스케줄러 오류 발생", e);
		}
	}
}
