import os
import requests
import pandas as pd
from dotenv import load_dotenv
import hmac
import hashlib
import time
from typing import Dict, Any, Optional

class BybitFuturesTrader:
    def __init__(self, testnet: bool = True):
        """
        Bybit Futures 트레이딩 클래스 초기화
        
        :param testnet: 테스트넷 사용 여부 (기본값: True)
        """
        load_dotenv()  # .env 파일에서 환경변수 로드
        
        self.base_url = "https://api-testnet.bybit.com" if testnet else "https://api.bybit.com"
        self.api_key = os.getenv('BYBIT_ACCESS_KEY')
        self.secret_key = os.getenv('BYBIT_SECRET_KEY')
        
        if not self.api_key or not self.secret_key:
            raise ValueError("API 키와 시크릿 키를 .env 파일에서 찾을 수 없습니다.")

    def _generate_signature(self, params: Dict[str, Any]) -> str:
        """
        API 요청을 위한 서명 생성
        
        :param params: 서명에 사용될 파라미터
        :return: 생성된 서명
        """
        sorted_params = sorted(params.items(), key=lambda x: x[0])
        signature_payload = '&'.join([f"{k}={v}" for k, v in sorted_params])
        signature_payload += f"&timestamp={int(time.time() * 1000)}"
        
        signature = hmac.new(
            self.secret_key.encode('utf-8'), 
            signature_payload.encode('utf-8'), 
            hashlib.sha256
        ).hexdigest()
        
        return signature

    def get_account_balance(self) -> Dict[str, Any]:
        """
        계정 잔고 조회
        
        :return: 계정 잔고 정보
        """
        endpoint = "/v5/account/account-info"
        timestamp = int(time.time() * 1000)
        
        params = {
            "api_key": self.api_key,
            "timestamp": timestamp,
        }
        
        signature = self._generate_signature(params)
        
        headers = {
            "X-BAPI-API-KEY": self.api_key,
            "X-BAPI-SIGN": signature,
            "X-BAPI-TIMESTAMP": str(timestamp),
            "X-BAPI-RECV-WINDOW": "5000"
        }
        
        response = requests.get(
            f"{self.base_url}{endpoint}", 
            headers=headers, 
            params=params
        )
        
        return response.json()

    def place_order(self, 
                    symbol: str, 
                    side: str, 
                    order_type: str,
                    qty: float, 
                    price: Optional[float] = None) -> Dict[str, Any]:
        """
        선물 주문 실행
        
        :param symbol: 거래 심볼 (예: BTCUSDT)
        :param side: 주문 방향 ('Buy' 또는 'Sell')
        :param order_type: 주문 타입 (예: 'Market', 'Limit')
        :param qty: 주문 수량
        :param price: 지정가 주문 시 가격 (선택사항)
        :return: 주문 결과
        """
        endpoint = "/v5/order/create"
        timestamp = int(time.time() * 1000)
        
        params = {
            "api_key": self.api_key,
            "category": "linear",  # 선물 거래
            "symbol": symbol,
            "side": side,
            "order_type": order_type,
            "qty": str(qty),
            "timestamp": timestamp,
        }
        
        # 지정가 주문의 경우 가격 추가
        if order_type == 'Limit' and price:
            params['price'] = str(price)
        
        signature = self._generate_signature(params)
        
        headers = {
            "X-BAPI-API-KEY": self.api_key,
            "X-BAPI-SIGN": signature,
            "X-BAPI-TIMESTAMP": str(timestamp),
            "X-BAPI-RECV-WINDOW": "5000",
            "Content-Type": "application/json"
        }
        
        response = requests.post(
            f"{self.base_url}{endpoint}", 
            headers=headers, 
            json=params
        )
        
        return response.json()

    def get_market_data(self, 
                        symbol: str, 
                        interval: str = '15', 
                        limit: int = 200) -> pd.DataFrame:
        """
        시장 데이터 조회 (캔들 데이터)
        
        :param symbol: 거래 심볼 (예: BTCUSDT)
        :param interval: 캔들 간격 (분 단위, 기본값 15분)
        :param limit: 최대 조회 데이터 수 (기본값 200)
        :return: 캔들 데이터 DataFrame
        """
        endpoint = "/v5/market/kline"
        
        params = {
            "category": "linear",
            "symbol": symbol,
            "interval": interval,
            "limit": limit
        }
        
        response = requests.get(
            f"{self.base_url}{endpoint}", 
            params=params
        )
        
        data = response.json().get('result', {}).get('list', [])
        
        # 캔들 데이터를 DataFrame으로 변환
        df = pd.DataFrame(data, columns=[
            'start_time', 'open', 'high', 'low', 'close', 
            'volume', 'turnover'
        ])
        
        # 데이터 타입 변환
        numeric_columns = ['open', 'high', 'low', 'close', 'volume', 'turnover']
        df[numeric_columns] = df[numeric_columns].astype(float)
        df['start_time'] = pd.to_datetime(df['start_time'], unit='ms')
        
        return df

class RiskManager:
    def __init__(self, trader: BybitFuturesTrader):
        """
        리스크 관리 클래스
        
        :param trader: BybitFuturesTrader 객체
        """
        self.trader = trader
        self.balance = None
    
    def update_balance(self):
        """
        계정 잔고 업데이트
        """
        self.balance = self.trader.get_account_balance()
    
    def calculate_position_size(self, 
                                total_risk_percentage: float = 0.01, 
                                stop_loss_percentage: float = 0.02) -> float:
        """
        포지션 규모 계산
        
        :param total_risk_percentage: 전체 자본 중 리스크 감수 비율 (기본 1%)
        :param stop_loss_percentage: 손절매 비율 (기본 2%)
        :return: 포지션 규모
        """
        if not self.balance:
            self.update_balance()
        
        total_balance = float(self.balance['result']['list'][0]['totalAvailableBalance'])
        risk_amount = total_balance * total_risk_percentage
        
        # 손절매 비율을 고려한 포지션 규모 계산
        position_size = risk_amount / stop_loss_percentage
        
        return position_size

class DatabaseManager:
    def __init__(self, 
                 host='localhost', 
                 user='root', 
                 password=None, 
                 database='bybit_trading'):
        """
        MySQL 데이터베이스 관리 클래스
        
        :param host: 데이터베이스 호스트
        :param user: 데이터베이스 사용자
        :param password: 데이터베이스 비밀번호
        :param database: 데이터베이스 이름
        """
        import mysql.connector
        
        self.connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        self.cursor = self.connection.cursor()
    
    def save_trade_data(self, trade_data: Dict[str, Any]):
        """
        거래 데이터 저장
        
        :param trade_data: 저장할 거래 데이터
        """
        query = """
        INSERT INTO trades 
        (symbol, side, order_type, quantity, price, timestamp) 
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        
        values = (
            trade_data.get('symbol', ''),
            trade_data.get('side', ''),
            trade_data.get('order_type', ''),
            trade_data.get('qty', 0),
            trade_data.get('price', 0),
            trade_data.get('timestamp', time.time())
        )
        
        self.cursor.execute(query, values)
        self.connection.commit()
    
    def close_connection(self):
        """
        데이터베이스 연결 종료
        """
        self.cursor.close()
        self.connection.close()

# 사용 예시
def main():
    # 테스트넷 트레이더 초기화
    trader = BybitFuturesTrader(testnet=True)
    
    # 리스크 관리자 초기화
    risk_manager = RiskManager(trader)
    
    # 데이터베이스 매니저 초기화 (비밀번호는 환경변수에서 로드)
    db_manager = DatabaseManager(password=os.getenv('MYSQL_PASSWORD'))
    
    try:
        # 계정 잔고 조회
        balance = trader.get_account_balance()
        print("계정 잔고:", balance)
        
        # 포지션 규모 계산
        position_size = risk_manager.calculate_position_size()
        print("계산된 포지션 규모:", position_size)
        
        # 시장 데이터 조회
        market_data = trader.get_market_data(symbol='BTCUSDT')
        print("최근 시장 데이터:\n", market_data.head())
        
        # 주문 실행 예시 (실제 주문은 주석 처리)
        # order = trader.place_order(
        #     symbol='BTCUSDT', 
        #     side='Buy', 
        #     order_type='Market', 
        #     qty=position_size
        # )
        # print("주문 결과:", order)
        
        # 거래 데이터 저장 예시
        # db_manager.save_trade_data(order)
        
    except Exception as e:
        print(f"오류 발생: {e}")
    
    finally:
        # 데이터베이스 연결 종료
        db_manager.close_connection()

if __name__ == "__main__":
    main()