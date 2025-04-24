package org.example.auctionmarketevent.common.message;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class AuctionEndMessage {
    private Long auctionId;
    private Long amount;
    private String username;
}
