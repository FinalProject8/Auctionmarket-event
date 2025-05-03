package org.example.auctionmarketevent.springbatch.job.dto;

import java.time.Instant;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
@JsonPropertyOrder({ // 빅쿼리의 컬럼 순서와 맞추기 위해 Java 객체를 JSON 으로 직렬화할 때 필드 순서를 지정
	"auctionId",
	"productId",
	"productName",
	"productCategory",
	"maxPrice",
	"auctionStartTime",
	"auctionEndTime"
})
public class AuctionsWinningBidDto {

	// BigQuery 테이블 컬럼명과 필드명을 일치시키거나,
	// @JsonProperty 를 사용하여 JSON 직렬화 시 사용할 이름 지정 가능
	@JsonProperty("auction_id")
	private Long auctionId;

	@JsonProperty("product_id")
	private Long productId;

	@JsonProperty("product_name")
	private String productName;

	@JsonProperty("product_category")
	private String productCategory;

	@JsonProperty("max_price")
	private Long maxPrice;

	@JsonProperty("auction_start_time")
	private Instant auctionStartTime;

	@JsonProperty("auction_end_time")
	private Instant auctionEndTime;

	@JsonIgnore  // JsonIgnore: JSON 직렬화에서 제외됨
	private Instant lastModified; // 타임스탬프 리스너 로직을 위해 존재
}

