# -*- coding: utf-8 -*-
"""
===================================
A股全市场扫描器
===================================

职责:
1. 获取全A股股票列表
2. 批量获取实时行情数据
3. 根据自定义条件筛选股票
4. 返回符合条件的股票列表

使用示例:
    scanner = StockScanner()
    results = scanner.scan_market(
        conditions={
            'change_pct': ('>', 3.0),      # 涨幅 > 3%
            'volume_ratio': ('>', 1.5),     # 量比 > 1.5
            'turnover_rate': ('>', 5.0),    # 换手率 > 5%
        }
    )
"""

import logging
from typing import Dict, List, Tuple, Optional, Callable
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class StockCondition:
    """股票筛选条件"""
    field: str          # 字段名: change_pct, volume_ratio, turnover_rate, price, market_cap
    operator: str       # 操作符: >, <, >=, <=, ==, between
    value: float        # 比较值
    value2: float = 0   # 用于between操作符的第二个值


@dataclass
class ScanResult:
    """扫描结果"""
    code: str
    name: str
    price: float
    change_pct: float       # 涨跌幅%
    volume_ratio: float     # 量比
    turnover_rate: float    # 换手率%
    market_cap: float       # 总市值(亿)
    volume: float          # 成交量
    amount: float          # 成交额


class StockScanner:
    """全市场股票扫描器"""
    
    def __init__(self):
        self._cache = {}
    
    def get_all_a_stocks(self) -> List[Dict]:
        """
        获取全A股列表
        
        Returns:
            股票列表,每个元素包含代码和名称
        """
        try:
            import akshare as ak
            
            logger.info("正在获取全A股列表...")
            # 获取A股实时行情数据
            df = ak.stock_zh_a_spot_em()
            
            stocks = []
            for _, row in df.iterrows():
                stock = {
                    'code': row.get('代码', ''),
                    'name': row.get('名称', ''),
                    'price': float(row.get('最新价', 0)),
                    'change_pct': float(row.get('涨跌幅', 0)),
                    'volume_ratio': float(row.get('量比', 0)),
                    'turnover_rate': float(row.get('换手率', 0)),
                    'market_cap': float(row.get('总市值', 0)) / 100000000,  # 转换为亿
                    'volume': float(row.get('成交量', 0)),
                    'amount': float(row.get('成交额', 0)),
                    'amplitude': float(row.get('振幅', 0)),
                    'peak_52w': float(row.get('52周最高', 0)),
                    'low_52w': float(row.get('52周最低', 0)),
                }
                stocks.append(stock)
            
            logger.info(f"成功获取 {len(stocks)} 只A股数据")
            return stocks
            
        except Exception as e:
            logger.error(f"获取A股列表失败: {e}")
            return []
    
    def parse_conditions(self, condition_str: str) -> List[StockCondition]:
        """
        解析条件字符串
        
        Args:
            condition_str: 条件字符串,如 "涨幅>3,量比>1.5,换手率>5"
        
        Returns:
            条件列表
        """
        conditions = []
        
        # 字段名映射
        field_map = {
            '涨幅': 'change_pct',
            '涨跌幅': 'change_pct',
            'change': 'change_pct',
            'change_pct': 'change_pct',
            
            '量比': 'volume_ratio',
            'volume_ratio': 'volume_ratio',
            'vr': 'volume_ratio',
            
            '换手率': 'turnover_rate',
            'turnover': 'turnover_rate',
            'turnover_rate': 'turnover_rate',
            'tr': 'turnover_rate',
            
            '价格': 'price',
            'price': 'price',
            
            '市值': 'market_cap',
            'market_cap': 'market_cap',
            'cap': 'market_cap',
            
            '振幅': 'amplitude',
            'amplitude': 'amplitude',
            
            '成交量': 'volume',
            'volume': 'volume',
            
            '成交额': 'amount',
            'amount': 'amount',
        }
        
        # 操作符映射
        op_map = {
            '>=': '>=',
            '<=': '<=',
            '==': '==',
            '=': '==',
            '>': '>',
            '<': '<',
        }
        
        # 解析条件
        parts = condition_str.split(',')
        for part in parts:
            part = part.strip()
            if not part:
                continue
            
            # 查找操作符
            matched_op = None
            for op in ['>=', '<=', '==', '>', '<']:
                if op in part:
                    matched_op = op
                    break
            
            if not matched_op:
                logger.warning(f"无法解析条件: {part}")
                continue
            
            # 分割字段和值
            idx = part.index(matched_op)
            field_str = part[:idx].strip()
            value_str = part[idx + len(matched_op):].strip()
            
            # 转换字段名
            field = field_map.get(field_str, field_str)
            if field not in field_map.values():
                logger.warning(f"未知字段: {field_str}")
                continue
            
            # 转换值
            try:
                value = float(value_str)
            except ValueError:
                logger.warning(f"无效的值: {value_str}")
                continue
            
            conditions.append(StockCondition(
                field=field,
                operator=matched_op,
                value=value
            ))
        
        return conditions
    
    def match_conditions(self, stock: Dict, conditions: List[StockCondition]) -> bool:
        """
        检查股票是否符合所有条件
        
        Args:
            stock: 股票数据
            conditions: 条件列表
        
        Returns:
            是否符合所有条件
        """
        for cond in conditions:
            stock_value = stock.get(cond.field, 0)
            
            if cond.operator == '>':
                if not (stock_value > cond.value):
                    return False
            elif cond.operator == '<':
                if not (stock_value < cond.value):
                    return False
            elif cond.operator == '>=':
                if not (stock_value >= cond.value):
                    return False
            elif cond.operator == '<=':
                if not (stock_value <= cond.value):
                    return False
            elif cond.operator == '==':
                if not (stock_value == cond.value):
                    return False
        
        return True
    
    def scan_market(self, conditions: Optional[Dict] = None, 
                   condition_str: Optional[str] = None,
                   limit: int = 100) -> List[ScanResult]:
        """
        扫描全市场,筛选符合条件的股票
        
        Args:
            conditions: 条件字典,如 {'change_pct': ('>', 3.0)}
            condition_str: 条件字符串,如 "涨幅>3,量比>1.5"
            limit: 返回结果数量限制
        
        Returns:
            符合条件的股票列表
        """
        # 解析条件
        if condition_str:
            cond_list = self.parse_conditions(condition_str)
        elif conditions:
            cond_list = []
            for field, (op, value) in conditions.items():
                cond_list.append(StockCondition(field=field, operator=op, value=value))
        else:
            cond_list = []
        
        # 获取全市场数据
        all_stocks = self.get_all_a_stocks()
        if not all_stocks:
            return []
        
        # 筛选股票
        matched_stocks = []
        for stock in all_stocks:
            if self.match_conditions(stock, cond_list):
                matched_stocks.append(ScanResult(
                    code=stock['code'],
                    name=stock['name'],
                    price=stock['price'],
                    change_pct=stock['change_pct'],
                    volume_ratio=stock['volume_ratio'],
                    turnover_rate=stock['turnover_rate'],
                    market_cap=stock['market_cap'],
                    volume=stock['volume'],
                    amount=stock['amount'],
                ))
        
        # 限制数量
        if limit > 0:
            matched_stocks = matched_stocks[:limit]
        
        logger.info(f"扫描完成: 共 {len(all_stocks)} 只股票, 符合 {len(matched_stocks)} 只")
        
        return matched_stocks
    
    def format_results(self, results: List[ScanResult]) -> str:
        """
        格式化扫描结果为文本
        
        Args:
            results: 扫描结果列表
        
        Returns:
            格式化后的文本
        """
        if not results:
            return "未找到符合条件的股票"
        
        lines = []
        lines.append(f"🔍 全市场扫描结果 (共 {len(results)} 只)\n")
        lines.append("=" * 80)
        lines.append(f"{'代码':<10} {'名称':<10} {'价格':<8} {'涨幅%':<8} {'量比':<8} {'换手率%':<8} {'市值亿':<10}")
        lines.append("=" * 80)
        
        for r in results:
            lines.append(
                f"{r.code:<10} {r.name:<10} {r.price:<8.2f} {r.change_pct:<8.2f} "
                f"{r.volume_ratio:<8.2f} {r.turnover_rate:<8.2f} {r.market_cap:<10.1f}"
            )
        
        lines.append("=" * 80)
        return "\n".join(lines)
