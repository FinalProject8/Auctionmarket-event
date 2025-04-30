package org.example.auctionmarketevent.common.listener;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.connection.Message;
import org.example.auctionmarketevent.common.message.AuctionEndMessage;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;

@Slf4j
@Service
@RequiredArgsConstructor
public class RedisExpirationListener implements MessageListener {

    private final WebClient webClient;

    @Override
    public void onMessage(Message message, byte[] patten) {
        String expiredKey = message.toString();
        log.info("TTL 만료 감지: {}", expiredKey);

        if (expiredKey.startsWith("auction:end:")) {
            String auctionId = expiredKey.split(":")[2];
            String socketUrl = "http://localhost:8081/internal/auction/" + auctionId + "/winner";

            webClient.post()
                    .uri(socketUrl)
                    .retrieve()
                    .toBodilessEntity()
                    .subscribe(
                            success -> log.info("웹소캣 서버에 낙찰가 정보 전송 요청 완료: auctionId = {}", auctionId),
                            error -> log.error("웹소캣 서버 요청 실패: auctionId = {}, error = {}", auctionId, error.getMessage())
                    );
        }
    }
}
