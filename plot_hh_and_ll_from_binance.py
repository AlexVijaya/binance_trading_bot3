#
import os
from if_asset_is_close_to_hh_or_ll import find_asset_close_to_hh_and_ll
def plot_hh_and_ll_from_binance ():
    potential_higher_high_assets, potential_lower_low_assets=find_asset_close_to_hh_and_ll()

    print(potential_higher_high_assets, potential_lower_low_assets)


    from plot_levels_with_plotly_binance_symbols import plot_levels_with_plotly
    from pathlib import Path
    for trading_pair in potential_higher_high_assets:
        where_to_plot = os.path.join ( os.getcwd () ,
                                       'datasets' ,
                                       'plots' ,
                                       'binance_plots_hh' ,
                                       f'{trading_pair}.png')

        path_to_databases = os.path.join ( os.getcwd () , 'datasets' , 'plots' , 'binance_plots_hh' )
        Path ( path_to_databases ).mkdir ( parents = True , exist_ok = True )


        plot_levels_with_plotly(trading_pair,where_to_plot,path_to_databases)

    for trading_pair in potential_lower_low_assets:
        where_to_plot = os.path.join ( os.getcwd () ,
                                       'datasets' ,
                                       'plots' ,
                                       'binance_plots_ll' ,
                                       f'{trading_pair}.png')

        path_to_databases = os.path.join ( os.getcwd () , 'datasets' , 'plots' , 'binance_plots_ll' )
        Path ( path_to_databases ).mkdir ( parents = True , exist_ok = True )


        plot_levels_with_plotly(trading_pair,where_to_plot,path_to_databases)

    from yield_binance_trading_pair import yield_binance_trading_pair
    from pathlib import Path
    # yield_binance_trading_pair ()
    # for symbol in yield_binance_trading_pair ():
    #     print ( 'part after yield_binance_trading_pair is at work' )
    #     print ( "symbol" , symbol )
    #     where_to_plot_var = os.path.join ( os.getcwd () ,
    #                                        'datasets' ,
    #                                        'plots' ,
    #                                        'binance_plots' ,
    #                                        f'{symbol}.png' )
    #     path_to_databases = os.path.join ( os.getcwd () , 'datasets' , 'plots' , 'binance_plots' )
    #     Path ( path_to_databases ).mkdir ( parents = True , exist_ok = True )
    #
    #     plot_levels_with_plotly ( symbol , where_to_plot,path_to_databases )

plot_hh_and_ll_from_binance()