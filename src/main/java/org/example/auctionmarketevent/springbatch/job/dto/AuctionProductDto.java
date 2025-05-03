package org.example.auctionmarketevent.springbatch.job.dto;

import java.math.BigDecimal;
import java.sql.Timestamp;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class AuctionProductDto {

	private Long auctionId;
	private Long productId;
	private String productName;
	private String productCategory;
	private BigDecimal maxPrice;
	private Timestamp auctionStartTime;
	private Timestamp auctionEndTime;
	private Timestamp lastModified;
}
