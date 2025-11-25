# -*- coding: utf-8 -*-
"""
Created on Wed Jul  5 10:42:09 2023

@author: doylef
"""
from pyenergyplus.api import EnergyPlusAPI
import pyenergyplus
import sys
import os
import time
import datetime as dt
from datetime import datetime
#import conversion.conversion as reflect_conv
import custom.conversion as reflect_conv
import custom.callback as reflect_callbk
import logging
logger = logging.getLogger(__name__)


class EpManager:
    def __init__(self, digital_twin):
        self.dtwin = digital_twin
        self.config = digital_twin.config
        self.got_handles = False
        self.ep_api = EnergyPlusAPI()
        self.ep_state = self.ep_api.state_manager.new_state()
        self.proceed_with_step_logic = False
        #print('calling api')
        #self.ep_api.runtime.callback_begin_zone_timestep_after_init_heat_balance(self.ep_state, self.ep_callback)
        
        # Register callbacks
        self.ep_api.runtime.callback_begin_new_environment(self.ep_state, self.begin_new_environment)
        self.ep_api.runtime.callback_after_component_get_input(self.ep_state, self.after_component_get_input)
        self.ep_api.runtime.callback_after_new_environment_warmup_complete(self.ep_state, self.after_new_environment_warmup_complete)
        self.ep_api.runtime.callback_begin_zone_timestep_before_init_heat_balance(self.ep_state, self.begin_zone_timestep_before_init_heat_balance)

        self.ep_api.runtime.callback_after_predictor_before_hvac_managers(self.ep_state, self.after_predictor_before_hvac_managers)
        self.ep_api.runtime.callback_after_predictor_after_hvac_managers(self.ep_state, self.after_predictor_after_hvac_managers)
        self.ep_api.runtime.callback_begin_system_timestep_before_predictor(self.ep_state, self.begin_system_timestep_before_predictor)
        self.ep_api.runtime.callback_begin_zone_timestep_after_init_heat_balance(self.ep_state, self.begin_zone_timestep_after_init_heat_balance)
        self.ep_api.runtime.callback_end_system_sizing(self.ep_state, self.end_system_sizing)
        self.ep_api.runtime.callback_end_system_timestep_after_hvac_reporting(self.ep_state, self.end_system_timestep_after_hvac_reporting)
        self.ep_api.runtime.callback_end_system_timestep_before_hvac_reporting(self.ep_state, self.end_system_timestep_before_hvac_reporting)
        self.ep_api.runtime.callback_end_zone_sizing(self.ep_state, self.end_zone_sizing)
        self.ep_api.runtime.callback_end_zone_timestep_before_zone_reporting(self.ep_state, self.end_zone_timestep_before_zone_reporting)
        self.ep_api.runtime.callback_end_zone_timestep_after_zone_reporting(self.ep_state, self.end_zone_timestep_after_zone_reporting)
        self.ep_api.runtime.callback_inside_system_iteration_loop(self.ep_state, self.inside_system_iteration_loop)
        self.ep_api.runtime.callback_message(self.ep_state, self.message)
        self.ep_api.runtime.callback_progress(self.ep_state, self.progress)
        self.ep_api.runtime.callback_unitary_system_sizing(self.ep_state, self.unitary_system_sizing)


        self.custom_input_file_path = os.path.join(self.dtwin.working_directory, 'dt_in.idf') 
        self.simulation_datetime = None

    def begin_new_environment(self, state):
        #print("#callback_begin_new_environment called#")
        if self.proceed_with_step_logic:
            self.setActuators("begin_new_environment")
            self.run_custom("begin_new_environment")
            self.collectSensorData("begin_new_environment")
        return

    def after_component_get_input(self, state):
        #print("#callback_after_component_get_input called#")
        if self.proceed_with_step_logic:
            self.setActuators("after_component_get_input")
            self.run_custom("after_component_get_input")
            self.collectSensorData("after_component_get_input")
        return

    def after_new_environment_warmup_complete(self, state):
        #print("#callback_after_new_environment_warmup_complete called#")
        if self.proceed_with_step_logic:
            self.setActuators("after_new_environment_warmup_complete")
            self.run_custom("after_new_environment_warmup_complete")
            self.collectSensorData("after_new_environment_warmup_complete")
        return

    def after_predictor_after_hvac_managers(self, state):
        #print("#callback_after_predictor_after_hvac_managers called#")
        if self.proceed_with_step_logic:
            self.setActuators("after_predictor_after_hvac_managers")
            self.run_custom("after_predictor_after_hvac_managers")
            self.collectSensorData("after_predictor_after_hvac_managers")
        return

    def begin_system_timestep_before_predictor(self, state):
        #print("#callback_begin_system_timestep_before_predictor called#")
        if self.proceed_with_step_logic:
            self.setActuators("begin_system_timestep_before_predictor")
            self.run_custom("begin_system_timestep_before_predictor")
            self.collectSensorData("begin_system_timestep_before_predictor")
        return

    def begin_zone_timestep_after_init_heat_balance(self, state):
        #print("#callback_begin_zone_timestep_after_init_heat_balance called#")
        if self.proceed_with_step_logic:
            self.setActuators("begin_zone_timestep_after_init_heat_balance")
            self.run_custom("begin_zone_timestep_after_init_heat_balance")
            self.collectSensorData("begin_zone_timestep_after_init_heat_balance")
        return

    def end_system_sizing(self, state):
        #print("#callback_end_system_sizing called#")
        if self.proceed_with_step_logic:
            self.setActuators("end_system_sizing")
            self.run_custom("end_system_sizing")
            self.collectSensorData("end_system_sizing")
        return

    def end_system_timestep_after_hvac_reporting(self, state):
        #print("#callback_end_system_timestep_after_hvac_reporting called#")
        if self.proceed_with_step_logic:
            self.setActuators("end_system_timestep_after_hvac_reporting")
            self.run_custom("end_system_timestep_after_hvac_reporting")
            self.collectSensorData("end_system_timestep_after_hvac_reporting")
        return

    def end_system_timestep_before_hvac_reporting(self, state):
        #print("#callback_end_system_timestep_before_hvac_reporting called#")
        if self.proceed_with_step_logic:
            self.setActuators("end_system_timestep_before_hvac_reporting")
            self.run_custom("end_system_timestep_before_hvac_reporting")
            self.collectSensorData("end_system_timestep_before_hvac_reporting")
        return

    def end_zone_sizing(self, state):
        #print("#callback_end_zone_sizing called#")
        if self.proceed_with_step_logic:
            self.setActuators("end_zone_sizing")
            self.run_custom("end_zone_sizing")
            self.collectSensorData("end_zone_sizing")
        return

    def end_zone_timestep_before_zone_reporting(self, state):
        #print("#callback_end_zone_timestep_before_zone_reporting called#")
        if self.proceed_with_step_logic:
            self.setActuators("end_zone_timestep_before_zone_reporting")
            self.run_custom("end_zone_timestep_before_zone_reporting")
            self.collectSensorData("end_zone_timestep_before_zone_reporting")
        return

    def inside_system_iteration_loop(self, state):
        #print("#callback_inside_system_iteration_loop called#")
        if self.proceed_with_step_logic:
            self.setActuators("inside_system_iteration_loop")
            self.run_custom("inside_system_iteration_loop")
            self.collectSensorData("inside_system_iteration_loop")
        return

    def message(self, state):
        #print("#callback_message called#")
        if self.proceed_with_step_logic:
            self.setActuators("message")
            self.run_custom("message")
            self.collectSensorData("message")
        return

    def progress(self, state):
        #print("#callback_progress called#")
        if self.proceed_with_step_logic:
            self.setActuators("progress")
            self.run_custom("progress")
            self.collectSensorData("progress")
        return

    def unitary_system_sizing(self, state):
        #print("#callback_unitary_system_sizing called#")
        if self.proceed_with_step_logic:
            self.setActuators("unitary_system_sizing")
            self.run_custom("unitary_system_sizing")
            self.collectSensorData("unitary_system_sizing")
        return

    def setCurrentSimulationTime(self):
        #EP year has issues with reporting year in regard to weather file
        #todo add logic to handle year change on 12/31-1/1 
        year = self.dtwin.start_year
        month = self.ep_api.exchange.month(self.ep_state)
        day = self.ep_api.exchange.day_of_month(self.ep_state)
        hour = self.ep_api.exchange.hour(self.ep_state)
        minute = self.ep_api.exchange.minutes(self.ep_state)

        timedelta = dt.timedelta()
        #EP occasionally returns hours/minutes beyond 24/60 that need to 
        #be adjusted
        if hour >= 24.0:
            hour = 23.0
            timedelta += dt.timedelta(hours=1)
         
        if minute >= 60.0:
            minute = 59   
            timedelta += dt.timedelta(minutes=1)
            
        dtime = dt.datetime(year=year, month=month, day=day, hour=hour, minute=minute)
        dtime += timedelta
        self.simulation_datetime = dtime
     
    def collectSensorData(self, timepoint):
        for idx in self.dtwin.sensors_df.index:
            curr_sensor_handle = self.dtwin.sensors_df['ep_handle'][idx]
            if self.dtwin.sensors_df['Read_stage'][idx] == timepoint:
                if self.dtwin.sensors_df['Type'][idx] == 'sensor':
                    value =  self.ep_api.exchange.get_variable_value(self.ep_state, curr_sensor_handle)
                    self.dtwin.sensors_df.iloc[idx, self.dtwin.sensors_df.columns.get_loc('current_val')] = value
                elif self.dtwin.sensors_df['Type'][idx] == 'meter':
                    value =  self.ep_api.exchange.get_meter_value(self.ep_state, curr_sensor_handle)
                    self.dtwin.sensors_df.iloc[idx, self.dtwin.sensors_df.columns.get_loc('current_val')] = value
    '''
    def get_actuator_values_by_signals(self):
        self.dtwin.get_signals_for_timepoint(self.simulation_datetime)
        for idx in self.dtwin.signals_df.index:
            curr_signal_tagname = self.dtwin.signals_df['SignalTagName'][idx]
            curr_signal_value = self.dtwin.signals_df['current_val'][idx]
            curr_conversion =  self.dtwin.signals_df['conversion_func'][idx] 
            if curr_conversion != "none":
                conversion_func = getattr(reflect_conv, curr_conversion)
                curr_signal_value = conversion_func(self.config, self.simulation_datetime, curr_signal_value)
            self.dtwin.actuators_df.loc[self.dtwin.actuators_df['SourceTagName'] == curr_signal_tagname, 'current_val'] = curr_signal_value
    '''
    def get_actuator_values_by_signals(self):
        self.dtwin.get_signals_for_timepoint(self.simulation_datetime)
        for idx in self.dtwin.actuators_df.index:
            curr_source_tagname = self.dtwin.actuators_df['SourceTagName'][idx]
            curr_conversion =  self.dtwin.actuators_df['ConversionFunction'][idx] 
            curr_signal_value = self.dtwin.signals_df.loc[self.dtwin.signals_df['SignalTagName'] == curr_source_tagname, 'current_val'].iloc[0]
            if curr_conversion != "none":
                conversion_func = getattr(reflect_conv, curr_conversion)
                curr_signal_value = conversion_func(self.config, self.simulation_datetime, curr_signal_value)
            self.dtwin.actuators_df.iloc[idx , self.dtwin.actuators_df.columns.get_loc('current_val')] = curr_signal_value    


    def setActuators(self, timepoint):
            for idx in self.dtwin.actuators_df.index:
                if self.dtwin.actuators_df['Override_stage'][idx] == timepoint:
                    print("calling api to set actuator with(state, {},{} at {})".format(self.dtwin.actuators_df['ep_handle'][idx], self.dtwin.actuators_df['current_val'][idx], timepoint))
                    self.ep_api.exchange.set_actuator_value(self.ep_state, self.dtwin.actuators_df['ep_handle'][idx], self.dtwin.actuators_df['current_val'][idx])
                    
    def set_actuator_handles(self):
        for idx in self.dtwin.actuators_df.index:
            curr_actuator_category = self.dtwin.actuators_df['ActuatorCategory'][idx]
            curr_actuator_name = self.dtwin.actuators_df['ActuatorName'][idx]
            curr_actuator_instance = self.dtwin.actuators_df['ActuatorInstance'][idx]
            self.dtwin.actuators_df.iloc[idx, self.dtwin.actuators_df.columns.get_loc('ep_handle')] = self.ep_api.exchange.get_actuator_handle(self.ep_state, curr_actuator_category, curr_actuator_name, curr_actuator_instance)
        
        print(self.dtwin.actuators_df)
        
        if -1 in self.dtwin.actuators_df['ep_handle'].values:
            return False
        else:
            return True
        
    def set_sensor_handles(self):
        for idx in self.dtwin.sensors_df.index:
            curr_sensor_name = self.dtwin.sensors_df['SensorName'][idx]
            curr_sensor_instance = self.dtwin.sensors_df['SensorInstance'][idx]
            curr_sensor_type = self.dtwin.sensors_df['Type'][idx]
            if  curr_sensor_type == 'sensor':
                self.dtwin.sensors_df.iloc[idx, self.dtwin.sensors_df.columns.get_loc('ep_handle')] = self.ep_api.exchange.get_variable_handle(self.ep_state, curr_sensor_name, curr_sensor_instance)
            elif curr_sensor_type == 'meter':
                self.dtwin.sensors_df.iloc[idx, self.dtwin.sensors_df.columns.get_loc('ep_handle')] = self.ep_api.exchange.get_meter_handle(self.ep_state, curr_sensor_name)
            
            print(self.dtwin.sensors_df)
        if -1 in self.dtwin.sensors_df['ep_handle'].values:
            return False
        else:
            return True
        
    
    def begin_zone_timestep_before_init_heat_balance(self, state):
        #print("#begin_zone_timestep_before_init_heat_balance called#")
        self.proceed_with_step_logic = False
        self.ep_state = state
        if not self.got_handles:
            if not self.ep_api.exchange.api_data_fully_ready(self.ep_state):
                return
            else:
                self.got_handles = self.set_sensor_handles() and self.set_actuator_handles()
                if not self.got_handles:
                    print("!!!Unable to get sensor or actuator handle(s)!!!")
                    
                    self.ep_api.runtime.stop_simulation(self.ep_state)
                    sys.exit(1)
        elif self.ep_api.exchange.warmup_flag(self.ep_state):
            return
          
        else:

            self.setCurrentSimulationTime()
            #todo make this buffer period configurable and put in 
            time_buffer = dt.timedelta()
            time_buffer = dt.timedelta(minutes=5)
            while datetime.now() < (self.simulation_datetime + time_buffer):
                time.sleep(5)
            #as this is the first callback per simulation iteration
            #perform the following two lines that affect the rest of the callbacks
            self.proceed_with_step_logic = True
            
            #this call manages actuator signal values used by all subsequent callbacks in timeperiod
            self.get_actuator_values_by_signals()
            
            self.setActuators("begin_zone_timestep_before_init_heat_balance")

            self.run_custom("begin_zone_timestep_before_init_heat_balance")
            self.collectSensorData("begin_zone_timestep_before_init_heat_balance")
            #sensors and meters values are not persisted until end_zone_timestep_after_zone_reporting is called
            
            
    def run_custom(self, timeperiod):
        #custom_callbacks_df
        for idx in self.dtwin.custom_callbacks_df.index:
            curr_row_timeperiod = self.dtwin.custom_callbacks_df['TimePeriod'][idx]
            #print(curr_row_timeperiod)
            if  curr_row_timeperiod == timeperiod:
                curr_callback = self.dtwin.custom_callbacks_df['Function'][idx]
                custom_func = getattr(reflect_callbk, curr_callback)
                custom_func(self.dtwin)
        
    def after_predictor_before_hvac_managers(self, state):
        #print("#after_predictor_before_hvac_managers called#")
        if self.proceed_with_step_logic:
            pass
        else:
            return       
            
    def end_zone_timestep_after_zone_reporting(self, state):
        #print("#end_zone_timestep_after_zone_reporting called#")
        if self.proceed_with_step_logic:
            self.setActuators("end_zone_timestep_after_zone_reporting")
            self.collectSensorData("end_zone_timestep_after_zone_reporting")
            self.dtwin.store_simulated_signals(self.simulation_datetime)
        else:
            return

                  
    def prep_input_file_for_simulation(self):
        '''
        overrides the building model idf file RunPeriod to match requested time period 
        inefficient...look for better way to do this via API
        '''
        with open(self.config.get('ENERGYPLUS', 'EPBuildingModel'), 'rt') as base_input_model_file:
            with open(self.custom_input_file_path, 'wt') as custom_input_model_file:
                for line in base_input_model_file: 

                    found_eof = False        
                    while not found_eof:
                        line = base_input_model_file.readline()
                        if not line:
                            found_eof = True
                        else:
                            if line.startswith('RunPeriod'):
                                custom_input_model_file.write(line)
                                #should not find eof before close of section, but just in case...
                                while not ';' in line and not found_eof:
                                    line = base_input_model_file.readline()
                                    if not line:
                                        found_eof = True
                                        break
                                    else:
                                        found_key = False
                                        for key in self.dtwin.override_map.keys():
                                            if key in line:
                                                custom_input_model_file.write(self.dtwin.override_map[key])
                                                found_key = True
                                                break
                                        if not found_key:
                                            custom_input_model_file.write(line)
                                      
                            else:
                                custom_input_model_file.write(line)
                                


            
    def invoke_simulation(self):
        self.prep_input_file_for_simulation();
        for idx in self.dtwin.sensors_df.index:
            self.ep_api.exchange.request_variable(self.ep_state, self.dtwin.sensors_df['SensorName'][idx], self.dtwin.sensors_df['SensorInstance'][idx])
           
        self.ep_api.runtime.run_energyplus(self.ep_state,
                                   [
                                       '-w', self.config.get('ENERGYPLUS', 'EPWeatherFile'),
                                       '-d', 'out',
                                       self.custom_input_file_path
                                       
                                       ]
                                   )
    
