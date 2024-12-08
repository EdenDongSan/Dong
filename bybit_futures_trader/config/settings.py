import os
from dotenv import load_dotenv
from typing import Dict, Any

class Config:
    """
    프로젝트 전역 설정 관리 클래스
    """
    def __init__(self, env_file: str = '.env'):
        """
        설정 초기화
        
        :param env_file: 환경변수 파일 경로
        """
        # .env 파일 로드
        load_dotenv(dotenv_path=env_file)
        
        # Bybit API 설정
        self.BYBIT_API_CONFIG: Dict[str, Any] = {
            'testnet': os.getenv('BYBIT_TESTNET', 'true').lower() == 'true',
            'base_url_testnet': 'https://api-testnet.bybit.com',
            'base_url_mainnet': 'https://api.bybit.com',
            'api_key': os.getenv('BYBIT_ACCESS_KEY'),
            'secret_key': os.getenv('BYBIT_SECRET_KEY'),
        }
        
        # 데이터베이스 설정
        self.DATABASE_CONFIG: Dict[str, Any] = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'user': os.getenv('DB_USER', 'root'),
            'password': os.getenv('DB_PASSWORD'),
            'database': os.getenv('DB_NAME', 'bybit_trading'),
            'port': int(os.getenv('DB_PORT', 3306))
        }
        
        # 트레이딩 전략 설정
        self.TRADING_CONFIG: Dict[str, Any] = {
            'symbol': os.getenv('TRADING_SYMBOL', 'BTCUSDT'),
            'leverage': float(os.getenv('TRADING_LEVERAGE', 10)),
            'risk_percentage': float(os.getenv('RISK_PERCENTAGE', 0.01)),
            'stop_loss_percentage': float(os.getenv('STOP_LOSS_PERCENTAGE', 0.02))
        }
        
        # 로깅 설정
        self.LOGGING_CONFIG: Dict[str, Any] = {
            'level': os.getenv('LOG_LEVEL', 'INFO'),
            'file_path': os.getenv('LOG_FILE', 'trading_log.log')
        }
        
        # 검증 및 오류 처리
        self._validate_config()
    
    def _validate_config(self):
        """
        설정 값 검증
        """
        # API 키 검증
        if not self.BYBIT_API_CONFIG['api_key'] or not self.BYBIT_API_CONFIG['secret_key']:
            raise ValueError("Bybit API 키와 시크릿 키를 .env 파일에서 찾을 수 없습니다.")
        
        # 데이터베이스 설정 검증
        if not self.DATABASE_CONFIG['password']:
            raise ValueError("데이터베이스 비밀번호가 설정되지 않았습니다.")
        
        # 리스크 설정 검증
        if (self.TRADING_CONFIG['risk_percentage'] <= 0 or 
            self.TRADING_CONFIG['risk_percentage'] > 1):
            raise ValueError("리스크 퍼센테이지는 0과 1 사이여야 합니다.")
    
    def get_bybit_base_url(self) -> str:
        """
        Bybit API 기본 URL 반환
        
        :return: Bybit API 기본 URL
        """
        return (self.BYBIT_API_CONFIG['base_url_testnet'] 
                if self.BYBIT_API_CONFIG['testnet'] 
                else self.BYBIT_API_CONFIG['base_url_mainnet'])
    
    def get_config(self, config_type: str) -> Dict[str, Any]:
        """
        특정 유형의 설정 반환
        
        :param config_type: 설정 유형 ('bybit', 'database', 'trading', 'logging')
        :return: 해당 유형의 설정 딕셔너리
        """
        config_map = {
            'bybit': self.BYBIT_API_CONFIG,
            'database': self.DATABASE_CONFIG,
            'trading': self.TRADING_CONFIG,
            'logging': self.LOGGING_CONFIG
        }
        
        return config_map.get(config_type, {})

# 전역 설정 인스턴스 생성
config = Config()