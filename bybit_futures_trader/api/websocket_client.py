import json
import time
import hmac
import hashlib
import base64
import websockets
import asyncio
from typing import Dict, Any, Optional, Callable, List
from config.settings import config
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BybitWebsocketClient:
    """Bitget WebSocket 클라이언트"""
    
    def __init__(self, use_testnet: bool = False):
        """
        WebSocket 클라이언트 초기화
        """
        self.api_config = config.get_config('bybit')
        self.ws_public_url = "wss://ws.bitget.com/v2/ws/public"
        self.ws_private_url = "wss://ws.bitget.com/v2/ws/private"
        self.callbacks = {}
        self.subscribed_channels = set()
        self.ws = None
        self.is_connected = False
        self.last_ping_time = time.time()
        self.last_pong_time = time.time()
        self.ping_interval = 20  # 20초마다 ping
        self.connection_timeout = 30  # 30초 동안 pong이 없으면 재연결
        self.max_channels_per_conn = 50  # 권장 최대 채널 수
        self.message_rate_limit = 10  # 초당 최대 메시지 수
        self.message_count = 0
        self.message_time = time.time()
        
    def _generate_signature(self) -> str:
        """
        API 요청 서명 생성
        """
        timestamp = str(int(time.time()))
        message = timestamp + 'GET' + '/user/verify'
        signature = hmac.new(
            self.api_config['secret_key'].encode('utf-8'),
            message.encode('utf-8'),
            hashlib.sha256
        )
        return base64.b64encode(signature.digest()).decode('utf-8')

    async def _connect(self, is_private: bool = False):
        """
        웹소켓 연결 설정
        
        :param is_private: private 채널 사용 여부
        """
        url = self.ws_private_url if is_private else self.ws_public_url
        
        try:
            self.ws = await websockets.connect(url)
            self.is_connected = True
            logger.info(f"WebSocket {'private' if is_private else 'public'} 채널 연결 성공")
            
            if is_private:
                await self._authenticate()
            
            # 연결 유지를 위한 태스크들 시작
            asyncio.create_task(self._keep_alive())
            asyncio.create_task(self._monitor_connection())
            
        except Exception as e:
            self.is_connected = False
            logger.error(f"WebSocket 연결 실패: {str(e)}")
            raise

    async def _authenticate(self):
        """private 채널 인증"""
        timestamp = str(int(time.time()))
        sign = self._generate_signature()
        
        auth_message = {
            "op": "login",
            "args": [{
                "apiKey": self.api_config['api_key'],
                "passphrase": self.api_config['passphrase'],
                "timestamp": timestamp,
                "sign": sign
            }]
        }
        
        await self._send_message(auth_message)
        response = await self.ws.recv()
        auth_response = json.loads(response)
        
        if auth_response.get('code') != '0':
            raise Exception(f"인증 실패: {auth_response.get('msg')}")
        
        logger.info("WebSocket 인증 성공")

    async def _keep_alive(self):
        """연결 유지를 위한 ping 전송"""
        while self.is_connected:
            if time.time() - self.last_ping_time >= self.ping_interval:
                try:
                    await self._send_message("ping")
                    self.last_ping_time = time.time()
                except Exception as e:
                    logger.error(f"Ping 전송 실패: {str(e)}")
                    await self._reconnect()
            await asyncio.sleep(1)

    async def _monitor_connection(self):
        """연결 상태 모니터링"""
        while self.is_connected:
            if time.time() - self.last_pong_time > self.connection_timeout:
                logger.warning("Pong 응답 timeout, 재연결 시도")
                await self._reconnect()
            await asyncio.sleep(1)

    async def _reconnect(self):
        """재연결 처리"""
        self.is_connected = False
        try:
            if self.ws:
                await self.ws.close()
            
            await self._connect(len(self.subscribed_channels) > 0)
            
            # 기존 구독 채널 복구
            if self.subscribed_channels:
                await self.subscribe(list(self.subscribed_channels))
                
        except Exception as e:
            logger.error(f"재연결 실패: {str(e)}")
            # 지수 백오프로 재시도
            await asyncio.sleep(5)
            await self._reconnect()

    async def _send_message(self, message):
        """
        메시지 전송 (rate limit 적용)
        """
        current_time = time.time()
        
        # 초당 메시지 수 제한 확인
        if current_time - self.message_time >= 1:
            self.message_count = 0
            self.message_time = current_time
            
        if self.message_count >= self.message_rate_limit:
            await asyncio.sleep(1)
            self.message_count = 0
            self.message_time = time.time()
            
        if isinstance(message, str):
            await self.ws.send(message)
        else:
            await self.ws.send(json.dumps(message))
            
        self.message_count += 1

    async def subscribe(self, channels: List[Dict[str, str]], callback: Callable):
        """
        채널 구독
        
        :param channels: 구독할 채널 목록
        :param callback: 메시지 처리 콜백 함수
        """
        if len(self.subscribed_channels) + len(channels) > self.max_channels_per_conn:
            raise Exception(f"최대 채널 구독 수({self.max_channels_per_conn})를 초과했습니다.")
            
        if not self.is_connected:
            await self._connect()
            
        subscribe_message = {
            "op": "subscribe",
            "args": channels
        }
        
        await self._send_message(subscribe_message)
        
        for channel in channels:
            channel_key = f"{channel['instType']}.{channel['channel']}.{channel['instId']}"
            self.callbacks[channel_key] = callback
            self.subscribed_channels.add(channel_key)

    async def unsubscribe(self, channels: List[Dict[str, str]]):
        """
        채널 구독 해제
        
        :param channels: 구독 해제할 채널 목록
        """
        unsubscribe_message = {
            "op": "unsubscribe",
            "args": channels
        }
        
        await self._send_message(unsubscribe_message)
        
        for channel in channels:
            channel_key = f"{channel['instType']}.{channel['channel']}.{channel['instId']}"
            self.callbacks.pop(channel_key, None)
            self.subscribed_channels.discard(channel_key)

    async def _handle_messages(self):
        """메시지 처리 루프"""
        while self.is_connected:
            try:
                message = await self.ws.recv()
                
                # ping/pong 처리
                if message == 'pong':
                    self.last_pong_time = time.time()
                    continue
                    
                data = json.loads(message)
                
                # 에러 처리
                if data.get('event') == 'error':
                    logger.error(f"에러 수신: {data}")
                    continue
                
                # 구독 메시지 처리
                if 'arg' in data and 'data' in data:
                    channel_info = data['arg']
                    channel_key = (f"{channel_info['instType']}."
                                 f"{channel_info['channel']}."
                                 f"{channel_info['instId']}")
                    
                    if channel_key in self.callbacks:
                        await self.callbacks[channel_key](data)
                        
            except websockets.exceptions.ConnectionClosed:
                logger.warning("WebSocket 연결이 종료되었습니다.")
                await self._reconnect()
            except Exception as e:
                logger.error(f"메시지 처리 중 오류 발생: {str(e)}")

    async def start(self):
        """WebSocket 클라이언트 시작"""
        while True:
            try:
                await self._handle_messages()
            except Exception as e:
                logger.error(f"처리 중 오류 발생: {str(e)}")
                await asyncio.sleep(5)

    async def close(self):
        """WebSocket 연결 종료"""
        self.is_connected = False
        if self.ws:
            await self.ws.close()