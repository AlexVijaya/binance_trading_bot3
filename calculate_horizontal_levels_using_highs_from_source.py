import os
import time
import traceback
from collections import Counter
import numpy as np
import sys
import sqlite3
import pandas as pd


def calculate_horizontal_levels_using_only_highs_binance(symbol,
                                                 round_to_this_number_of_decimal_places,
                                                 number_of_same_highs,
                                                 level_for_this_period,
                                                 source='binance'):
    orininal_stdout=sys.stdout
    with open("standard_output_to_this_file.txt", "a") as stdout_file:
        start_time=time.time()
        conn=sqlite3.connect(os.path.join(os.getcwd() ,'datasets','sql_databases', 'binance_historical_data.db'))
        cur=conn.cursor()
        conn.row_factory=sqlite3.Row

        #cur.execute('''select * from stock_prices where stock_id=10 order by id''')
        stock_df = pd.read_sql ( f'''select * from (select * from crypto_assets_ohlc
                                                where trading_pair="{symbol}"
                                                order by open_time Desc limit {level_for_this_period})
                                                order by open_time ASC;''' ,
                                 conn , parse_dates = 'index' )


        # stock_df=pd.read_sql(f'''select * from yf_gerchik_tickers_and_prices where symbol="{symbol}" ;''',
        #                      conn,parse_dates = 'Date')

        stock_df_rounded = stock_df.round ( {'open': round_to_this_number_of_decimal_places ,
                                             'high': round_to_this_number_of_decimal_places ,
                                             'low': round_to_this_number_of_decimal_places ,
                                             'close': round_to_this_number_of_decimal_places} )
        stock_df_rounded_ohlc = stock_df_rounded.loc[: , ['index' , 'open' , 'high' , 'low' , 'close']]

        # stock_df_rounded_ohlc_with_equal_highs_keep_false = \
        #     stock_df_rounded_ohlc[stock_df_rounded_ohlc.duplicated ( subset = ["high"] ,keep = False)]

        stock_df_rounded_ohlc_with_equal_highs_keep_false = stock_df_rounded_ohlc

        stock_df_rounded_ohlc_with_equal_highs_keep_false_sorted=\
            stock_df_rounded_ohlc_with_equal_highs_keep_false.sort_values ( by = "index" )

        stock_series_rounded_ohlc_with_equal_highs_keep_false=\
            stock_df_rounded_ohlc_with_equal_highs_keep_false['high'].value_counts()
        df_count_high_values=stock_series_rounded_ohlc_with_equal_highs_keep_false.to_frame ()

        df_count_high_values.reset_index(level=0, inplace=True)


        df_count_high_values.rename({'high':'number_of_same_highs', 'index':'high'}, axis='columns', inplace=True)
        df_count_high_values_merged=pd.merge(stock_df_rounded_ohlc_with_equal_highs_keep_false_sorted,
                                            df_count_high_values[['high','number_of_same_highs']],
                                            on='high',how='left' )
        count_high_values_merged_full_df=pd.merge(stock_df_rounded,
                                                 df_count_high_values_merged[['high', 'number_of_same_highs']],
                                                 on='high' ,how='left')

        count_high_values_merged_full_df.drop_duplicates(subset='index',inplace=True)
        count_high_values_merged_full_df.reset_index(inplace = True,drop=True)
        count_high_values_merged_full_df.update(stock_df)

        count_high_values_merged_full_df["NaN_highs"] = np.nan




        levels_with_touches=\
            count_high_values_merged_full_df.loc[count_high_values_merged_full_df["number_of_same_highs"]>=number_of_same_highs]
        #print("levels_with_touches\n",levels_with_touches)

        levels_with_touches_all_dates=pd.merge(count_high_values_merged_full_df.loc[:,["index","NaN_highs","high"]],
                                                levels_with_touches.loc[:,["index","high"]],
                                               on="index",how='left')

        levels_with_touches_all_dates.rename({'high_y':'high','high_x':'high_without_NaNs'},axis = 'columns', inplace = True)

        # for each_row in levels_with_touches_all_dates[levels_with_touches_all_dates["high"].notnull()]:
        #
        #     print("each_row\n", each_row)
        # print ('levels_with_touches_all_dates[levels_with_touches_all_dates["high"].notnull()]\n',
        #        levels_with_touches_all_dates[levels_with_touches_all_dates["high"].notnull()])
        try:
            levels_without_NaNs_df=levels_with_touches_all_dates[levels_with_touches_all_dates["high"].notnull()]
            list_of_valid_highs=list()
            for index_of_not_null_levels, row in levels_without_NaNs_df.iterrows():
                # print( "index_of_not_null_levels=",index_of_not_null_levels,"\n\n","\n\nrow=\n",row)
                # print("\n",levels_with_touches_all_dates.at[levels_with_touches_all_dates.index[index_of_not_null_levels],"high"])
                # print ( "\n" , count_high_values_merged_full_df.at[
                #     levels_with_touches_all_dates.index[index_of_not_null_levels] , "high"] )
                # print ( "\n" , count_high_values_merged_full_df.at[
                #     levels_with_touches_all_dates.index[index_of_not_null_levels]-1 , "high"] )
                # print ( "\n" , count_high_values_merged_full_df.at[
                #     levels_with_touches_all_dates.index[index_of_not_null_levels]+1 , "high"] )
                try:
                    if (index_of_not_null_levels-1)<0:
                        prev_high=count_high_values_merged_full_df.at[
                        levels_with_touches_all_dates.index[index_of_not_null_levels], "high"]
                        # print(f"(index_of_not_null_levels-1)<0 for {symbol}")
                        # print (index_of_not_null_levels-1)
                        # print("number_of_same_highs", number_of_same_highs)
                        #time.sleep(15)
                    else:
                        prev_high=count_high_values_merged_full_df.at[
                            levels_with_touches_all_dates.index[index_of_not_null_levels]-1, "high"]
                except Exception:
                    pass


                try:
                    if count_high_values_merged_full_df.at[
                        levels_with_touches_all_dates.index[index_of_not_null_levels]+1, "high"]:
                        next_high=count_high_values_merged_full_df.at[
                            levels_with_touches_all_dates.index[index_of_not_null_levels]+1, "high"]
                    else:
                        next_high = count_high_values_merged_full_df.at[
                            levels_with_touches_all_dates.index[index_of_not_null_levels] , "high"]
                except Exception:
                    pass


                current_high=count_high_values_merged_full_df.at[
                    levels_with_touches_all_dates.index[index_of_not_null_levels], "high"]
                if (prev_high <= current_high) and (next_high<=current_high):
                    print(f'for {symbol} {current_high} is a valid high on '
                         f'{count_high_values_merged_full_df.at[levels_with_touches_all_dates.index[index_of_not_null_levels],"index"]}',
                          file=stdout_file)
                    #print("\nlevel with touches\n",levels_with_touches)

                    list_of_valid_highs.append(current_high)
                else:
                    pass

            #for valid_high in list_of_valid_highs:
            print('levels_with_touches_before_is_in\n',
                  levels_with_touches.head(1000).to_string(),
                  file=stdout_file)
            levels_with_touches = \
                levels_with_touches[levels_with_touches['high'].isin(list_of_valid_highs)]
            print ( 'levels_with_touches_after_is_in\n' ,
                    levels_with_touches.head(1000).to_string(),
                    file=stdout_file)


            #print("\nlist of valid highs\n",list_of_valid_highs)

            dictitonary_of_high_levels_with_counter=dict(Counter(list_of_valid_highs))
            print ( "\ndictitonary_of_high_levels_with_counter\n" ,
                    dictitonary_of_high_levels_with_counter,
                    file=stdout_file)
            # inverted_dictitonary_of_high_levels_with_counter=dict((v,k) for k,v in dictitonary_of_high_levels_with_counter.items())
            # print ( "\ninverted_dictitonary_of_high_levels_with_counter\n" , inverted_dictitonary_of_high_levels_with_counter)
            for key, value in dictitonary_of_high_levels_with_counter.items():
                #print (f"key, value for {symbol}", key, value)
                if value == 1:
                    # print("\nlevels_with_touches_all_dates\n",levels_with_touches_all_dates.head(1000).to_string())
                    # levels_with_touches_all_dates.loc[(levels_with_touches_all_dates['high_without_NaNs']==key), ["high_without_NaNs"]]=None
                    # levels_with_touches_all_dates.loc[
                    #     (levels_with_touches_all_dates['high_without_NaNs'] == key) , ["high"]] = None
                    # print(f"I dropped {key} high of symbol {symbol}")
                    # print ( "\nlevels_with_touches_all_dates\n" , levels_with_touches_all_dates.head(1000).to_string() )
                    #
                    #
                    print("\nlevels_with_touches_before_drop\n",
                          levels_with_touches.head(10000).to_string()
                          ,file=stdout_file)
                    #print("I am going to delete this row\n", levels_with_touches.loc[levels_with_touches['high']==key])
                    #levels_with_touches=levels_with_touches.drop(levels_with_touches['high']==key)
                    levels_with_touches=levels_with_touches[levels_with_touches['high'] != key]
                    print ( "\nlevels_with_touches_after_drop\n" ,
                            levels_with_touches.head ( 10000 ).to_string (),
                            file=stdout_file)


        except Exception as e:
            print(e)
            traceback.print_exc()
            print(f"problem with {symbol}")
            #print(levels_without_NaNs_df)
            #time.sleep(20)

        #print ( "levels_with_touches_all_dates\n" , levels_with_touches_all_dates )
        count_high_values_merged_full_df['index'] = pd.to_datetime ( count_high_values_merged_full_df['index'] )
        count_high_values_merged_full_df.set_index ( 'index' , inplace = True )



        levels_with_touches_all_dates['index'] = pd.to_datetime ( levels_with_touches_all_dates['index'] )
        levels_with_touches_all_dates.set_index ( 'index' , inplace = True )
        #print ( "count_high_values_merged_full_df\n" , count_high_values_merged_full_df )
        #print(levels_with_touches['high'].tolist())
        #print(tuple([0.5]*len(levels_with_touches)))
        # print ( "count_high_values_merged_full_df\n" ,
        #         count_high_values_merged_full_df.head ( 10000 ).to_string () )
        print ( "levels_with_touches_end_of_program\n" ,
                levels_with_touches.head ( 10000 ).to_string (), file=stdout_file )
        # print ( "levels_with_touches_all_dates\n" ,
        #         levels_with_touches_all_dates.head ( 10000 ).to_string () )
        conn.close()
        return count_high_values_merged_full_df , levels_with_touches , levels_with_touches_all_dates
#calculate_horizontal_levels_using_only_highs_binance('BTCUSDT',1,2,1000)





