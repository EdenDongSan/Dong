import aiohttp
import base64
import hmac
import hashlib
import time
import logging
from logging_setup import APILogger
import json
from models import Position
from typing import Optional, Dict, List
from urllib.parse import urlencode

logger = logging.getLogger(__name__)

class BitgetAPI:
    def __init__(self, api_key: str, secret_key: str, passphrase: str):
        self.API_KEY = api_key
        self.SECRET_KEY = secret_key
        self.PASSPHRASE = passphrase
        self.api_logger = APILogger()
        self.BASE_URL = "https://api.bitget.com"
        self.session = None
        
    async def __aenter__(self):        # 쓴다.
        """Context manager entry - creates aiohttp session"""
        self.session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):     # 쓴다.
        """Context manager exit - closes aiohttp session"""
        if self.session:
            await self.session.close()
            
    def _generate_signature(self, timestamp: str, method: str,           # _create_headers 함수에서 호출당한다.
                          request_path: str, body: str = '') -> str:
        message = timestamp + method + request_path + body
        mac = hmac.new(
            bytes(self.SECRET_KEY, encoding='utf8'),
            bytes(message, encoding='utf-8'),
            digestmod='sha256'
        )
        return base64.b64encode(mac.digest()).decode()

    def _create_headers(self, method: str, request_path: str, body: str = '') -> dict:            # _request 함수에서 호출당한다.
        timestamp = str(int(time.time() * 1000))
        
        if '?' in request_path:
            base_path, query = request_path.split('?', 1)
            params = sorted(query.split('&'))
            request_path = base_path + '?' + '&'.join(params)
        
        message = timestamp + method.upper() + request_path + body
        signature = self._generate_signature(timestamp, method.upper(), request_path, body)
        
        return {
            "ACCESS-KEY": self.API_KEY,
            "ACCESS-SIGN": signature,
            "ACCESS-TIMESTAMP": timestamp,
            "ACCESS-PASSPHRASE": self.PASSPHRASE,
            "Content-Type": "application/json",
            "ACCESS-VERSION": "2"
        }

    async def _request(self, method: str, endpoint: str, params: dict = None, data: dict = None) -> Optional[dict]:   # 각종 api 요청에 호출된다.
        """통합된 비동기 HTTP 요청 처리"""
        if self.session is None:
            self.session = aiohttp.ClientSession()

        try:
            url = self.BASE_URL + endpoint
            query = ''
            
            if params:
                query = '?' + urlencode(sorted(params.items()))
                url = url + query

            headers = self._create_headers(   # 함수호출.
                method, 
                endpoint + query, 
                json.dumps(data) if data else ''
            )

            async with self.session.request(          #async with 를 사용해야 열고닫는게 가능하다. 비동기aiohttp 특징.
                method=method,
                url=url,
                headers=headers,
                json=data
            ) as response:
                response_data = await response.json()
                
                logger.info(f"API {method} {url} - Status: {response.status}")
                if response.status != 200:
                    logger.error(f"API Error: {response_data}")
                    
                return response_data

        except Exception as e:
            logger.error(f"Request error: {e}")
            return None
    
    async def get_historical_candles(self, symbol: str) -> Optional[dict]:             # 시작할 때 캐시를 api를 활용해서 받아오는 역할. 200개의 1분봉. data_web에서 호출당한다.
        """프로그램 시작 시점 기준 과거 200개의 1분봉 데이터 조회"""
        try:
            # 현재 시간을 밀리초로 변환
            end_time = str(int(time.time() * 1000))
            # 200분 전의 시간을 밀리초로 변환
            start_time = str(int(time.time() * 1000) - (200 * 60 * 1000))
            
            params = {
                'symbol': symbol,
                'granularity': '1m',
                'productType': 'USDT-FUTURES',
                'startTime': start_time,
                'endTime': end_time,
                'limit': '200'
            }
            
            return await self._request('GET', '/api/v2/mix/market/history-candles', params=params)
            
        except Exception as e:
            logger.error(f"Error fetching historical candles: {e}")
            return None

    async def set_leverage(self, symbol: str, leverage: int,         # 이거 open_position에서 호출하고싶은데 그러면 결괏값이 좀 병신이 됨. 그래서 order_execution 내부에 얘를 호출해서 bool값으로 결과를 반환하는 애를 만들어야함. 구현완료.
                      product_type: str = 'USDT-FUTURES',
                      margin_coin: str = 'USDT',
                      ) -> Optional[dict]:
        """비동기 레버리지 설정"""
        data = {
            'symbol': symbol.lower(),
            'productType': product_type,
            'marginCoin': margin_coin.upper(),
            'leverage': str(leverage)
        }
            
        return await self._request('POST', '/api/v2/mix/account/set-leverage', data=data)

    async def get_account_balance(self) -> Optional[dict]:                # trading_strategy_에서 주문을 실행하려고 포지션 계산을 할 때 계좌 잔고가 필요해서 호출당한다.
        """비동기 계좌 잔고 조회"""
        params = {'productType': 'USDT-FUTURES'}
        return await self._request('GET', '/api/v2/mix/account/accounts', params=params)

    async def get_position(self, symbol: str) -> Optional[Position]:            # 얘가 model.py에 있는 position 가져와서 만드는 핵심 position 얻기 함수이다.
        """비동기 포지션 정보 조회"""
        params = {
            'symbol': symbol,
            'marginMode': 'crossed',
            'productType': 'USDT-FUTURES',
            'marginCoin': 'USDT'
        }

        response = await self._request('GET', '/api/v2/mix/position/single-position', params=params)
        
        if response and response.get('code') == '00000' and response.get('data'):
            position_data = response['data'][0] if isinstance(response['data'], list) else response['data']
            
            if float(position_data.get('total', '0')) > 0:
                return Position(
                    symbol=symbol,
                    side='long' if position_data.get('holdSide') == 'long' else 'short',
                    size=float(position_data.get('total', '0')),
                    entry_price=float(position_data.get('openPriceAvg', '0')),
                    stop_loss_price=0.0,  # API에서 제공하지 않음
                    take_profit_price=0.0,  # API에서 제공하지 않음
                    timestamp=int(time.time() * 1000),
                    leverage=int(position_data.get('leverage', '1')),
                    
                    # 새로 추가된 필드들
                    break_even_price=float(position_data.get('breakEvenPrice', '0')),
                    unrealized_pl=float(position_data.get('unrealizedPL', '0')),
                    margin_size=float(position_data.get('marginSize', '0')),
                    available=float(position_data.get('available', '0')),
                    locked=float(position_data.get('locked', '0')),
                    liquidation_price=float(position_data.get('liquidationPrice', '0')),
                    margin_ratio=float(position_data.get('marginRatio', '0')),
                    mark_price=float(position_data.get('markPrice', '0')),
                    achieved_profits=float(position_data.get('achievedProfits', '0')),
                    total_fee=float(position_data.get('totalFee', '0')),
                    margin_mode=position_data.get('marginMode', 'crossed')
                )
        return None

    async def place_order(self, symbol: str, side: str, trade_side: str,            # order_exectuion에 open_position 함수에서 호출하는 함수. 실제 주문api 전송을 담당한다.
                         size: str, margin_coin: str = 'USDT', 
                         order_type: str = 'limit', price: str = None, 
                         trigger_price: str = None) -> dict:
        """비동기 주문 생성"""
        if price:
            price = str(round(float(price) * 10) / 10)
        if trigger_price:
            trigger_price = str(round(float(trigger_price) * 10) / 10)

        order_type_mapping = {
            'market': 'market',
            'limit': 'limit',
            'stop': 'profit_stop'
        }

        body = {
            "symbol": symbol,
            "productType": "USDT-FUTURES",
            "marginMode": "crossed",
            "marginCoin": margin_coin,
            "side": side,
            "tradeSide": trade_side,
            "orderType": order_type_mapping.get(order_type, 'limit'),
            "size": size,
        }

        if order_type == 'limit' and price:
            body["price"] = price
        elif order_type == 'stop' and trigger_price:
            body["triggerPrice"] = trigger_price
            body["holdSide"] = "short" if side == "buy" else "long"

        return await self._request('POST', '/api/v2/mix/order/place-order', data=body)

    async def place_tpsl_order(self, symbol: str, plan_type: str,  # open_postion 함수에서 호출당한다.
                             trigger_price: str, hold_side: str, size: str, 
                             execute_price: str = "0") -> dict:
        """비동기 스탑로스/테이크프로핏 주문 생성"""
        body = {
            "symbol": symbol.upper(),
            "marginCoin": "USDT",
            "productType": "USDT-FUTURES",
            "planType": plan_type,
            "triggerPrice": str(round(float(trigger_price) * 10) / 10),
            "triggerType": "mark_price",
            "executePrice": execute_price,
            "holdSide": hold_side,
            "size": size
        }

        return await self._request('POST', '/api/v2/mix/order/place-tpsl-order', data=body)

    async def close_position(self, symbol: str, margin_coin: str = 'USDT') -> dict: # 쓰인다. 시장가청산이다 이거.
        """비동기 포지션 청산"""
        body = {
            "symbol": symbol,
            "marginCoin": margin_coin,
            "productType": "USDT-FUTURES"
        }

        return await self._request('POST', '/api/v2/mix/order/close-positions', data=body)

    async def get_order_detail(self, symbol: str, order_id: str) -> dict:      # wait_for_order_fill 함수에서 쓰인다. order_execution.
        """비동기 주문 상태 조회"""
        params = {
            'symbol': symbol,
            'orderId': order_id
        }
        return await self._request('GET', '/api/v2/mix/order/detail', params=params)

    async def cancel_order(self, symbol: str, order_id: str) -> dict:      #미체결 주문 취소 함수.
        """비동기 주문 취소"""
        body = {
            "symbol": symbol,
            "orderId": order_id
        }
        return await self._request('POST', '/api/v2/mix/order/cancel-order', data=body)

    async def get_pending_orders(self, symbol: str = None,   #비동기미체결 주문이 진짜 있는지 확인시켜주는함수. 호출당한다. cancel_all_함수에 의해.
                               status: str = None, 
                               limit: str = "100") -> dict:
        """비동기 미체결 주문 조회"""
        params = {
            'productType': 'USDT-FUTURES',
            'limit': limit
        }
        
        if symbol:
            params['symbol'] = symbol
        if status:
            params['status'] = status

        return await self._request('GET', '/api/v2/mix/order/orders-pending', params=params)
    

    async def cancel_all_pending_orders(self, symbol: str) -> List[dict]:
        """비동기 30초 이상 지난 미체결 주문 취소"""
        results = []
        current_time_ms = int(time.time() * 1000)  # 현재 시간을 밀리초로 변환
        time_threshold_ms = 30 * 1000  # 30초를 밀리초로 변환
        
        pending_orders = await self.get_pending_orders(symbol)
        
        if pending_orders and pending_orders.get('code') == '00000':
            orders = pending_orders.get('data', {}).get('entrustedList', [])
            
            if not orders:
                return results  # 미체결 주문이 없을 경우 취소 시도 자체를 안함
                
            for order in orders:
                order_id = order.get('orderId')
                order_time = int(order.get('cTime', 0))  # 주문 생성 시간
                
                # 30초 이상 지난 주문만 취소
                if order_id and (current_time_ms - order_time >= time_threshold_ms):
                    result = await self.cancel_order(symbol, order_id)
                    if result and result.get('code') == '00000':
                        logger.info(f"미체결 주문 취소 성공: {order_id}, 경과 시간: {(current_time_ms - order_time)/1000:.1f}초")
                    else:
                        logger.error(f"미체결 주문 취소 실패: {order_id}, 경과 시간: {(current_time_ms - order_time)/1000:.1f}초")
                    results.append(result)
                else:
                    logger.debug(f"주문 유지: {order_id}, 경과 시간: {(current_time_ms - order_time)/1000:.1f}초")
                    
        return results