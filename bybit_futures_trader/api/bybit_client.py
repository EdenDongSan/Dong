import time
import hmac
import hashlib
import requests
import pandas as pd
from typing import Dict, Any, Optional, List
from config.settings import config

class BybitAPIException(Exception):
    """Bybit API 관련 예외 처리 클래스"""
    pass

class BybitClientV2:
    """Bybit API V2 기본 클라이언트 클래스"""
    def __init__(self):
        self.api_config = config.get_config('bybit')
        self.base_url = config.get_bybit_base_url()
        self.session = requests.Session()
    
    def _generate_signature(self, params: Dict[str, Any], timestamp: int) -> str:
        """
        API 요청 서명 생성
        
        :param params: 요청 파라미터
        :param timestamp: 타임스탬프
        :return: HMAC SHA256 서명
        """
        sorted_params = sorted(params.items(), key=lambda x: x[0])
        signature_payload = '&'.join([f"{k}={v}" for k, v in sorted_params])
        signature_payload += f"&timestamp={timestamp}"
        
        return hmac.new(
            self.api_config['secret_key'].encode('utf-8'),
            signature_payload.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()
    
    def _send_request(self, 
                      method: str, 
                      endpoint: str, 
                      params: Optional[Dict[str, Any]] = None, 
                      auth_required: bool = False) -> Dict[str, Any]:
        """
        API 요청 전송
        
        :param method: HTTP 메서드
        :param endpoint: API 엔드포인트
        :param params: 요청 파라미터
        :param auth_required: 인증 필요 여부
        :return: API 응답
        """
        url = f"{self.base_url}{endpoint}"
        headers = {}
        
        if auth_required:
            timestamp = int(time.time() * 1000)
            params = params or {}
            params['api_key'] = self.api_config['api_key']
            params['timestamp'] = timestamp
            
            signature = self._generate_signature(params, timestamp)
            headers.update({
                'X-BAPI-API-KEY': self.api_config['api_key'],
                'X-BAPI-SIGN': signature,
                'X-BAPI-TIMESTAMP': str(timestamp),
                'X-BAPI-RECV-WINDOW': '5000'
            })
        
        try:
            if method == 'GET':
                response = self.session.get(url, params=params, headers=headers)
            else:
                headers['Content-Type'] = 'application/json'
                response = self.session.post(url, json=params, headers=headers)
            
            data = response.json()
            
            if response.status_code != 200 or data.get('retCode') != 0:
                raise BybitAPIException(
                    f"API error: {data.get('retMsg')} (code: {data.get('retCode')})"
                )
            
            return data['result']
            
        except requests.exceptions.RequestException as e:
            raise BybitAPIException(f"Request failed: {str(e)}")

class SpotTradeClient(BybitClientV2):
    """현물 거래 API 클라이언트"""
    
    def get_market_price(self, symbol: str) -> Dict[str, Any]:
        """
        현재 시장 가격 조회
        
        :param symbol: 거래 심볼
        :return: 시장 가격 정보
        """
        return self._send_request(
            'GET',
            '/api/v2/spot/market/tickers',
            {'symbol': symbol}
        )
    
    def place_order(self, 
                    symbol: str,
                    side: str,
                    order_type: str,
                    qty: float,
                    price: Optional[float] = None) -> Dict[str, Any]:
        """
        주문 실행
        
        :param symbol: 거래 심볼
        :param side: 주문 방향 (Buy/Sell)
        :param order_type: 주문 유형 (Market/Limit)
        :param qty: 주문 수량
        :param price: 지정가 주문 가격
        :return: 주문 결과
        """
        params = {
            'symbol': symbol,
            'side': side,
            'orderType': order_type,
            'qty': str(qty)
        }
        
        if price and order_type == 'Limit':
            params['price'] = str(price)
            
        return self._send_request(
            'POST',
            '/api/v2/spot/trade/place-order',
            params,
            auth_required=True
        )

class FuturesTradeClient(BybitClientV2):
    """선물 거래 API 클라이언트"""
    
    def place_order(self,
                    symbol: str,
                    side: str,
                    order_type: str,
                    qty: float,
                    trade_side: str,
                    price: Optional[float] = None) -> Dict[str, Any]:
        """
        선물 주문 실행
        
        :param symbol: 거래 심볼
        :param side: 주문 방향 (Buy/Sell)
        :param order_type: 주문 유형 (Market/Limit)
        :param qty: 주문 수량
        :param trade_side: 거래 방향 (Open/Close)
        :param price: 지정가 주문 가격
        :return: 주문 결과
        """
        params = {
            'symbol': symbol,
            'side': side,
            'orderType': order_type,
            'qty': str(qty),
            'tradeSide': trade_side,
            'category': 'linear'
        }
        
        if price and order_type == 'Limit':
            params['price'] = str(price)
            
        return self._send_request(
            'POST',
            '/api/v2/mix/order/place-order',
            params,
            auth_required=True
        )
    
    def set_leverage(self, 
                     symbol: str,
                     leverage: int,
                     position_idx: Optional[int] = None) -> Dict[str, Any]:
        """
        레버리지 설정
        
        :param symbol: 거래 심볼
        :param leverage: 레버리지 배수
        :param position_idx: 포지션 인덱스 (one-way: 0, hedge: 1/2)
        :return: 설정 결과
        """
        params = {
            'symbol': symbol,
            'leverage': leverage,
            'category': 'linear'
        }
        
        if position_idx is not None:
            params['positionIdx'] = position_idx
            
        return self._send_request(
            'POST',
            '/api/v2/mix/account/set-leverage',
            params,
            auth_required=True
        )

class MarketDataClient(BybitClientV2):
    """시장 데이터 API 클라이언트"""
    
    def get_kline_data(self,
                       symbol: str,
                       interval: str = '15',
                       limit: int = 200,
                       start_time: Optional[int] = None,
                       end_time: Optional[int] = None) -> pd.DataFrame:
        """
        K라인(캔들) 데이터 조회
        
        :param symbol: 거래 심볼
        :param interval: 캔들 간격 (분 단위)
        :param limit: 조회할 캔들 개수
        :param start_time: 시작 시간 (timestamp in milliseconds)
        :param end_time: 종료 시간 (timestamp in milliseconds)
        :return: 캔들 데이터 DataFrame
        """
        params = {
            'symbol': symbol,
            'interval': interval,
            'limit': limit,
            'category': 'linear'
        }
        
        if start_time:
            params['startTime'] = start_time
        if end_time:
            params['endTime'] = end_time
            
        data = self._send_request('GET', '/api/v2/market/kline', params)
        
        if not data.get('list'):
            return pd.DataFrame()
            
        df = pd.DataFrame(
            data['list'],
            columns=['timestamp', 'open', 'high', 'low', 'close', 'volume', 'turnover']
        )
        
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        for col in ['open', 'high', 'low', 'close', 'volume', 'turnover']:
            df[col] = df[col].astype(float)
            
        return df.sort_values('timestamp')
