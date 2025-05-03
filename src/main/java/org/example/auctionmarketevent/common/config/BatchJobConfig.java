package org.example.auctionmarketevent.common.config;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import org.example.auctionmarketevent.common.listener.IncrementalTimestampStepListener;
import org.example.auctionmarketevent.common.provider.MySqlCustomPagingQueryProvider;
import org.example.auctionmarketevent.springbatch.job.dto.AuctionProductDto;
import org.example.auctionmarketevent.springbatch.job.dto.AuctionsWinningBidDto;
import org.example.auctionmarketevent.springbatch.job.reader.AuctionProductRowMapper;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.builder.JdbcPagingItemReaderBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

import lombok.extern.slf4j.Slf4j;

@Configuration
@Slf4j
public class BatchJobConfig {

	private final DataSource dataSource;
	private final int chunkSize;
	private final String jobName;

	// 작업 관리 + 기록 도구
	private final JobRepository jobRepository;
	private final PlatformTransactionManager transactionManager;

	private final ItemReader<AuctionProductDto> reader;
	private final ItemProcessor<AuctionProductDto, AuctionsWinningBidDto> processor;
	private final ItemWriter<AuctionsWinningBidDto> writer;
	private final IncrementalTimestampStepListener listener;

	// 생성자
	@Autowired
	public BatchJobConfig(DataSource dataSource,
		@Value("${app.batch.chunk-size}") int chunkSize,
		@Value("${app.batch.job-name}") String jobName,
		JobRepository jobRepository,
		PlatformTransactionManager transactionManager,
		ItemReader<AuctionProductDto> reader,
		ItemProcessor<AuctionProductDto, AuctionsWinningBidDto> processor,
		ItemWriter<AuctionsWinningBidDto> writer,
		IncrementalTimestampStepListener listener) {

		this.dataSource = dataSource;
		this.chunkSize = chunkSize;
		this.jobName = jobName;
		this.jobRepository = jobRepository;
		this.transactionManager = transactionManager;
		this.reader = reader;
		this.processor = processor;
		this.writer = writer;
		this.listener = listener;
	}

	// ItemReader 정의
	@Bean
	@StepScope
	public JdbcPagingItemReader<AuctionProductDto> mysqlItemReader(
		@Value("#{stepExecutionContext['lastProcessedTimestamp']}") Timestamp lastProcessedTimestamp
	) {

		// 데이터 가져올 SQL 쿼리 설정
		String baseSelect = "a.id AS auction_id, p.id AS product_id, p.product_name, p.category AS product_category, "
			+ "a.max_price, a.start_time AS auction_start_time, a.end_time AS auction_end_time, "
			+ "GREATEST(a.modified_at, p.modified_at) AS last_modified";
		String from = "auctions a JOIN product p ON a.product_id = p.id";
		String where = "GREATEST(a.modified_at, p.modified_at) > :lastProcessedTimestamp";

		MySqlCustomPagingQueryProvider queryProvider = new MySqlCustomPagingQueryProvider(baseSelect, from, where);

		Map<String, Object> parameterValues = new HashMap<>();

		// lastProcessedTimestamp 가 null 일 경우 기본값
		String defaultTimestamp = "2025-01-01 00:00:00";
		parameterValues.put("lastProcessedTimestamp",
			lastProcessedTimestamp == null ? defaultTimestamp : lastProcessedTimestamp);

		return new JdbcPagingItemReaderBuilder<AuctionProductDto>()
			.name("mysqlAuctionProductReader")
			.dataSource(this.dataSource)
			.queryProvider(queryProvider)
			.parameterValues(parameterValues)
			.pageSize(this.chunkSize)
			.rowMapper(new AuctionProductRowMapper())
			.maxItemCount(5000)
			.build();
	}

	// Reader, Processor, Writer, Listener 를 하나로 묶음
	@Bean
	public Step mysqlToBigQueryStep() {

		return new StepBuilder("mysqlToBigQueryStep", jobRepository)
			.<AuctionProductDto, AuctionsWinningBidDto>chunk(
				this.chunkSize,
				transactionManager // 실패하면 transactionManager 으로 롤백
			)
			.reader(this.reader)
			.processor(this.processor)
			.writer(this.writer)
			.listener(this.listener)
			.build();
	}

	// Step 을 묶어서 최종적인 하나의 완성된 Job 정의
	// 현재는 step 이 하나뿐이지만, 여러 개의 step 을 순서대로 연결할 수 있음
	@Bean
	public Job mysqlToBigQueryJob() {
		log.info("mysqlToBigQueryJob 빈을 생성: {}", this.jobName);

		return new JobBuilder(this.jobName, jobRepository)
			.incrementer(new RunIdIncrementer())
			.start(mysqlToBigQueryStep())
			.build();
	}
}
