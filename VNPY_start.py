import tushare as ts
import pymongo
from datetime import datetime

from vnpy.trader.object import BarData
from vnpy.trader.constant import Exchange
from vnpy.app.cta_strategy.template import OrderData, StopOrder

import numpy as np

from vnpy.app.cta_strategy.template import CtaTemplate

symbol = '510050'
exchange = 'SSE'
vtSymbol = '.'.join([symbol, exchange])

data = ts.get_hist_data(symbol, '2017-01-01')
data = data.sort_index().head()
print(data)

print('数据下载完成')

client = pymongo.MongoClient('localhost', 27017)
collection = client['DAILY_DB_NAME'][vtSymbol]
collection.create_index('datetime')

print('MongodDB连接成功')

for row in data.iterrows():
    date = row[0]
    data = row[1]

    bar = BarData(gateway_name='CTA', symbol=symbol, exchange=Exchange.SSE, datetime=datetime)

    bar.vt_symbol = vtSymbol
    bar.symbol = symbol
    bar.exchange = exchange
    bar.date = date
    bar.datetime = datetime.strptime(date, '%Y-%m-%d')
    bar.open_price = data['open']
    bar.high_price = data['high']
    bar.low_price = data['low']
    bar.close_price = data['close']
    bar.volume = data['volume']

    print(bar)

    flt = {'datetime': bar.datetime}
    collection.update_one(flt, {'$set': bar.__dict__}, upsert=True)

print('数据插入完成')


class DoubleMaStrategy(CtaTemplate):
    '双均线策略'
    className = 'DoubleMaStrategy'
    author = 'hjj'

    initDays = 25

    barCount = 0
    closeArray = np.zeros(20)
    ma5 = 0
    ma20 = 0
    lastMa5 = 0
    lastMa20 = 0

    parmList = ['name', 'className', 'author', 'vtSymbol']

    varList = ['inited', 'trading', 'pos']

    def __init__(self, ctaEngine, setting):
        super(DoubleMaStrategy, self).__init__(ctaEngine, setting)

        self.closeArray = np.zeros(20)

    def on_init(self):
        self.write_log('双均线策略初始化')

        initData = self.load_bar(self.initDays)

        for bar in initData:
            self.on_bar(bar)

        self.put_event()

    def on_start(self):
        self.write_log('双均线策略启动')
        self.put_event()

    def on_stop(self):
        self.write_log('双均线策略停止')
        self.put_event()

    def on_bar(self, bar: BarData):
        self.closeArray[0:19] = self.closeArray[1:20]
        self.closeArray[-1] = bar.close_price

        self.barCount += 1

        if self.barCount < self.initDays:
            return

        self.ma5 = self.closeArray[15:20].mean()
        self.ma20 = self.closeArray.mean()

        crossOver = self.ma5 > self.ma20 and self.lastMa5 <= self.lastMa20
        crossBelow = self.ma5 < self.ma20 and self.lastMa5 >= self.lastMa20

        if crossOver:
            if self.pos == 0:
                self.buy(bar.close_price * 1.05, 10000)
            elif self.pos < 0:
                self.cover(bar.close_price * 1.05, 10000)
                self.buy(bar.close_price * 1.05, 10000)
        elif crossBelow:
            if self.pos == 0:
                self.short(bar.close_price * 0.95, 10000)
            elif self.pos > 0:
                self.sell(bar.close_price * 0.95, 10000)
                self.short(bar.close_price * 0.95, 10000)

        self.put_event()

    def on_order(self, order: OrderData):
        pass

    def on_stop_order(self, stop_order: StopOrder):
        pass
