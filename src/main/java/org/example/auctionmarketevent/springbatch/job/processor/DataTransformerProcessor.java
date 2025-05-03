package org.example.auctionmarketevent.springbatch.job.processor;

import org.example.auctionmarketevent.springbatch.job.dto.AuctionProductDto;
import org.example.auctionmarketevent.springbatch.job.dto.AuctionsWinningBidDto;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class DataTransformerProcessor implements ItemProcessor<AuctionProductDto, AuctionsWinningBidDto> {

	@Override
	public AuctionsWinningBidDto process(AuctionProductDto item) throws Exception {

		// 데이터 변환
		AuctionsWinningBidDto bqDto = AuctionsWinningBidDto.builder()
			.auctionId(item.getAuctionId())
			.productId(item.getProductId())
			.productName(item.getProductName())
			.productCategory(item.getProductCategory())
			.maxPrice(item.getMaxPrice() != null ? item.getMaxPrice().longValue() : null) // 타입 변환 BigDecimal => Long
			.auctionStartTime(item.getAuctionStartTime() != null ? item.getAuctionStartTime().toInstant() : null)
			.auctionEndTime(item.getAuctionEndTime() != null ? item.getAuctionEndTime().toInstant() : null)
			.lastModified(item.getLastModified() != null ? item.getLastModified().toInstant() : null)
			.build();

		return bqDto;
	}
}
