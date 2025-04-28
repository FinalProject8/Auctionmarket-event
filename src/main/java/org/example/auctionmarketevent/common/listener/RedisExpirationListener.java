package org.example.auctionmarketevent.common.listener;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.connection.Message;
import org.example.auctionmarketevent.common.message.AuctionEndMessage;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

@Slf4j
@Service
@RequiredArgsConstructor
public class RedisExpirationListener implements MessageListener {

    private final RestTemplate restTemplate;

    @Override
    public void onMessage(Message message, byte[] patten) {
        String expiredKey = message.toString();
        log.info("TTL 만료 감지: {}", expiredKey);

        if (expiredKey.startsWith("auction:end:")) {
            String auctionId = expiredKey.split(":")[2];

            try {
                // 1. WebSocket 서버에서 낙찰자 정보 조회
                String socketUrl = "http://localhost:8081/internal/auction/" + auctionId + "/winner";
                restTemplate.postForEntity(socketUrl, null, AuctionEndMessage.class);

                log.info("웹소캣 서버에 낙찰자 정보 전송 명령 완료: {}", auctionId);
            } catch (Exception e) {
                log.error("웹소캣 서버에 낙찰자 정보 전송 명령 실패: {}", auctionId, e);
            }
        }
    }
}
