package org.example.auctionmarketevent.springbatch.job.runner;

import java.time.LocalDateTime;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Profile("dev")
@Slf4j
@Component
@RequiredArgsConstructor
public class JobRunner implements CommandLineRunner {

	private final JobLauncher jobLauncher;
	private final Job mysqlToBigQueryJob;

	// CommandLineRunner 인터페이스 메서드
	@Override
	public void run(String... args) throws Exception {

		// Job 실행을 위한 JobParameters 생성
		JobParameters jobParameters = new JobParametersBuilder()
			.addLocalDateTime("runTime", LocalDateTime.now()) // 실행 시간 파라미터
			.toJobParameters();

		try {
			jobLauncher.run(mysqlToBigQueryJob, jobParameters); // Job 실행
		} catch (Exception e) {
			log.error("JobRunner: 작업 실패", e);
		}
	}
}
