columns = ['OL_W_ID', 'OL_D_ID', 'OL_O_ID', 'OL_NUMBER', 'OL_I_ID', 'OL_DELIVERY_D', 'OL_AMOUNT', 'OL_SUPPLY_W_ID',
           'OL_QUANTITY', 'OL_DIST_INFO', 'S_W_ID', 'S_I_ID', 'S_QUANTITY', 'S_YTD', 'S_ORDER_CNT', 'S_DATA',
           'S_DIST_01', 'S_DIST_02', 'S_DIST_03', 'S_DIST_04', 'S_DIST_05', 'S_DIST_06', 'S_DIST_07', 'S_DIST_08',
           'S_DIST_09', 'S_DIST_10', 'I_ID', 'I_NAME', 'I_PRICE', 'I_DATA', 'I_IM_ID', 'S_REMOTE_CNT']

tab1 = ['OL_W_ID', 'OL_D_ID','OL_O_ID', 'OL_NUMBER', 'OL_DELIVERY_D', 'OL_AMOUNT', 'OL_QUANTITY', 'OL_DIST_INFO','S_W_ID', 'S_I_ID']
tab2 = ['ol_i_id','ol_supply_w_id', 'S_W_ID', 'S_I_ID', 's_quantity', 's_ytd', 's_data', 's_dist_01', 's_dist_02', 's_dist_03', 's_dist_04', 's_dist_05', 's_dist_06', 's_dist_07', 's_dist_08', 's_dist_09', 's_dist_10']
tab3 = ['S_I_ID', 'S_QUANTITY', 'S_YTD','s_order_cnt', 's_remote_cnt']
tab4 = ['S_I_ID', 'i_id', 'i_name', 'i_price', 'i_data', 'i_im_id']
a = []
for column in tab2:
    a.append(columns.index(column.upper()))

print(a)
