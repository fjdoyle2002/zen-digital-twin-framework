# -*- coding: utf-8 -*-
"""
Created on Mon Jun 26 14:43:38 2023

@author: doylef
"""
from seeq import spy
import pandas as pd
import logging
logger = logging.getLogger(__name__)

class SeeqRetrieval:
    def __init__(self, config, signals_df):
    
        SeeqServerURL=config.get('Seeq', 'SeeqServerURL')
        SeeqUser=config.get('Seeq', 'SeeqUser')   
        SeeqPassword=config.get('Seeq', 'SeeqPassword')
        SeeqRequestOrigin=config.get('Seeq', 'SeeqRequestOrigin')
        spy.login(url=SeeqServerURL, username=SeeqUser, password=SeeqPassword,request_origin_label=SeeqRequestOrigin)
        
        self.signals_df = signals_df
        self.items = pd.DataFrame()
        logger.info("SeeqRetrieval initilization, requesting Seeq items...")
        for idx in signals_df.index:
            if signals_df['SignalSource'][idx]=='seeq':
                new_items = spy.search({
                    'Datasource ID' : signals_df['SourceId'][idx],
                    'Data ID' : signals_df['SignalTagName'][idx]
                    })
                #self.items = self.items.append(new_items)
                self.items = pd.concat([self.items, new_items], ignore_index=True)
        logger.info(self.items)
       
    def retrieve_signals_for_actuators_at_timepoint(self, signals_df, timepoint):
        """
        function retrieves the physical building's signals of interest that will be used
        to override EP simulation actuators, and places values in the appropriate dataframes.
        """
        #todo note - grid="1min" used to ensure return value. Some timepoints return no values without this
        #...seems to be a failure to interpolate by default? Need to talk to Seeq
        data = spy.pull(self.items, start=timepoint, end=timepoint, grid="1min")
        for idx in signals_df.index:
            curr_signal_tagname = signals_df['SignalTagName'][idx]
            
            try:                
                curr_signal_value = data[curr_signal_tagname][0]
                #if the returned value is valid, set it in the dataframe
                if not pd.isna(curr_signal_value):
                    signals_df.iloc[idx, signals_df.columns.get_loc('current_val')] = curr_signal_value
                #otherwise, try again for that specific signal
                else:
                    logger.warning("Signal {} returned NaN value at time {}, attempting retry".format(curr_signal_tagname, timepoint))
                    retry_attempt = spy.pull(self.items[idx:idx+1], start=timepoint, end=timepoint, grid="1min")
                    curr_signal_value = retry_attempt[curr_signal_tagname][0]
                    #if still invalid, default to last value
                    if pd.isna(curr_signal_value):
                        logger.warning("Warning - signal {} returned NaN value on second attempt at time {}, retaining setting as last valid".format(curr_signal_tagname, timepoint))
                        curr_signal_value = -1
                    #otherwise, set the valid value
                    else:
                        signals_df.iloc[idx, signals_df.columns.get_loc('current_val')] = curr_signal_value

            except IndexError:
                #todo - better exception logic for lack of valid signal, and logging
                #currently leaving last value in place (obviously this is prone to error)
                logger.error("Unable to set signal for {} due to index error on Seeq pull results at time {}".format(curr_signal_tagname, timepoint))
                   
    
        
        