aaa = [' S_DIST_05', ' S_REMOTE_CNT', 'OL_SUPPLY_W_ID', ' C_CITY', 'OL_W_ID', ' S_ORDER_CNT', ' W_CITY',
       ' OL_DIST_INFO', ' C_DELIVERY_CNT', ' C_CREDIT', ' C_DISCOUNT', ' C_CREDIT_LIM', ' H_C_D_ID', ' H_DATA',
       ' W_ZIP', ' O_D_ID', ' O_ALL_LOCAL', ' D_ID', ' S_DIST_10', 'H_DATE', 'O_CARRIER_ID', ' W_STREET_1',
       ' NO_W_ID', ' W_ID', ' C_DATA', ' D_ZIP', ' O_OL_CNT', ' OL_QUANTITY', ' OL_W_ID', ' S_DATA', ' O_W_ID',
       ' I_NAME', ' W_STREET_2', ' C_MIDDLE', ' H_C_W_ID', ' C_STREET_1', ' C_STREET_2', 'C_D_ID', ' W_NAME',
       ' NO_O_ID', ' OL_AMOUNT', 'OL_I_ID', ' D_CITY', ' S_DIST_03', ' S_DIST_02', ' D_STREET_2', ' H_C_ID',
       ' C_PAYMENT_CNT', ' D_W_ID', ' OL_NUMBER', ' D_NAME', ' O_C_ID', ' C_FIRST', ' S_DIST_04', ' OL_D_ID',
       ' C_YTD_PAYMENT', ' NO_D_ID', ' I_PRICE', ' S_DIST_07', ' D_YTD', ' D_STATE', 'C_W_ID', ' H_W_ID',
       ' D_TAX', ' S_DIST_09', ' W_STATE', ' S_W_ID', ' I_IM_ID', ' OL_O_ID', ' O_ENTRY_D', 'OL_D_ID', ' W_YTD',
       ' C_LAST', ' I_ID', 'O_ID', ' C_BALANCE', ' W_TAX', ' OL_DELIVERY_D', ' S_DIST_01', ' OL_I_ID',
       ' I_DATA', ' S_I_ID', ' C_ZIP', ' C_PHONE', ' D_STREET_1', ' D_NEXT_O_ID', ' S_DIST_08', ' S_YTD',
       ' S_DIST_06', ' H_D_ID', 'C_ID', ' H_AMOUNT', ' C_SINCE', ' S_QUANTITY', ' C_STATE']
for i, attr in enumerate(aaa):
    aaa[i] = attr.strip()
f = open('./tpcc/Fun.txt')
lines = f.readlines()
print(lines)
b = set()
bbb = []

for attrs in lines:
    for attr in attrs.strip().split('[')[1].split(']')[0].split(','):
        bbb.append(aaa.index(attr.upper().strip()))
    print(bbb)
    bbb = []
