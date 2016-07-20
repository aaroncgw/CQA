from Data import YahooData
import numpy as np
import pandas as pd
from Data import Utility

universe = Utility.get_stock_universe('stock_universe.csv')
u_tick = universe['Tick'].unique().tolist()
universe.set_index('Tick', inplace=True)

returns = YahooData.get_returns(u_tick)
ev_ebitda = YahooData.get_ev_ebitda(u_tick)
ocf_ev = YahooData.get_ocf_ev(u_tick)
ratios = YahooData.get_ratios(u_tick, ['Price', 'PS', 'PB', 'PE', '50ma', '200ma'])
comp_info = YahooData.get_sector_industry(u_tick)

df = universe.join(comp_info)
df = df.join(ratios)
df = df.join(ev_ebitda)
df = df.join(ocf_ev)
df = df.join(returns)

df = df.replace('N/A', np.nan)
df['ma_ratio'] = df['50ma'].astype(float) / df['200ma'].astype(float)

df['ebitda_ev_rank'] = df['ebitda_ev'].astype(float).rank(ascending=True)
df['ocf_ev_rank'] = df['ocf_ev'].astype(float).rank(ascending=True)
df['PS_rank'] = df['PS'].astype(float).rank(ascending=False)
df['PB_rank'] = df['PB'].astype(float).rank(ascending=False)
df['PE_rank'] = df['PE'].astype(float).rank(ascending=False)
df['ma_ratio_rank'] = df['ma_ratio'].rank(ascending=True)
df['return_rank'] = df['1yr_rtn'].rank(ascending=True)

df['ebitda_ev_rank'].fillna(df['ebitda_ev_rank'].mean(), inplace=True)
df['ocf_ev_rank'].fillna(df['ocf_ev_rank'].mean(), inplace=True)
df['PS_rank'].fillna(df['PS_rank'].mean(), inplace=True)
df['PB_rank'].fillna(df['PB_rank'].mean(), inplace=True)
df['PE_rank'].fillna(df['PE_rank'].mean(), inplace=True)
df['ma_ratio_rank'].fillna(df['ma_ratio_rank'].mean(), inplace=True)
df['return_rank'].fillna(df['return_rank'].mean(), inplace=True)
df['value_rank_mean'] = df[['ebitda_ev_rank', 'PS_rank', 'PB_rank', 'PE_rank', 'ocf_ev_rank']].mean(axis=1)
df['mom_rank_mean'] = df[['ma_ratio_rank', 'return_rank']].mean(axis=1)

#df['score'] = np.sqrt(df['value_rank_mean'] * df['mom_rank_mean'])
df['score'] = 0.5 * (df['value_rank_mean'] + df['mom_rank_mean'])
df.sort_index(by='score', ascending=False, inplace=True)

df.to_csv(r'C:\Users\Guanwen\Google Drive\CQA\CQA_ValueMom_score.csv')