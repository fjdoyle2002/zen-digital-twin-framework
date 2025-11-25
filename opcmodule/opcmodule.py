import threading
import os
import pandas as pd
import time
import asyncio
import datetime as dt
from datetime import datetime, timezone
from asyncua import ua, Server
from asyncua.common.structures104 import new_enum
from opcmodule.opc_device import OPCDevice
import logging

class OPCUAModule:
    def __init__(self, working_directory, config):
        self.working_directory = working_directory
        self.config = config
        self._logger = logging.getLogger(__name__)

        self.server = None
        self.uri = None
        self.namespace = None
        self.should_run = True
        self.devices = []
        self.tagmap = {}
        self.sensors_df_reference = None
        self.sensors_updated = True

        devices_path = os.path.join(self.working_directory, self.config.get('CONFIGURATIONFILES', 'OpcDevicesFile'))
        self.opc_devices_df = pd.read_csv(devices_path)

        varibles_path = os.path.join(self.working_directory, self.config.get('CONFIGURATIONFILES', 'OpcVariablesFile'))
        self.opc_variables_df = pd.read_csv(varibles_path)
        self.opc_variables_df = self.opc_variables_df.assign(current_val=-1)
        #now, instantiate all OPC devices

        for idx in self.opc_devices_df.index:
            curr_opc_device_name = self.opc_devices_df['device_name'][idx]
            curr_opc_device_description = self.opc_devices_df['description'][idx]
            current_opc_device_class = self.opc_devices_df['class'][idx]
            #get subframe from variables for this device
            device_variables_df = self.opc_variables_df[self.opc_variables_df['device_name'] == curr_opc_device_name]
            
            current_device = OPCDevice(curr_opc_device_name, curr_opc_device_description, current_opc_device_class, device_variables_df)
            
            
            self.devices.append(current_device)
            
            
        self._logger.info('all opc devices instantiated')
    
    async def add_variables_to_devices(self):
        for curr_device in self.devices:
            await curr_device.add_variables(self.server, self.namespace, self.uri, self.tagmap)
        self._logger.info('all opc device nodes registered')


    #async def add_simulator_variables(self):
    #    for curr_device in self.devices:
    #        await self.tagmap | curr_device.add_variables(self.server, self.namespace, self.uri)
    #    self._logger.info('all opc device variables added')
    def update_variables(self, sensors_df):
        #for now we are passing in the object rather than registering it on initialization
        #in case it changes over time
        self.sensors_df_reference = sensors_df
        self.sensors_updated = True

   
    
    def retrieve_signals_for_actuators_at_timepoint(self, signals_df, timepoint):
        """
        function retrieves the physical building's signals of interest that will be used
        to override EP simulation actuators, and places values in the appropriate dataframes.
        Timepoint is irrelevant for OPC-UA retrieval, as values are read live from the OPC-UA server.
        """
        for idx in signals_df.index:
            if signals_df['SignalSource'][idx].lower() != 'opc':
            
                curr_signal_tagname = signals_df['SignalTagName'][idx]
                try:
                    curr_signal_value = self.tagmap[curr_signal_tagname].get_value()
                    #if the returned value is valid, set it in the dataframe
                    if curr_signal_value is not None:
                        signals_df.iloc[idx, signals_df.columns.get_loc('current_val')] = curr_signal_value
                    else:
                        self._logger.warning("Signal {} returned None value at time {}, retaining setting as last valid".format(curr_signal_tagname, timepoint))
                except KeyError:
                    self._logger.error("Signal {} not found in OPC-UA tag map".format(curr_signal_tagname))

    async def core(self):
        """Core asynchronous method to set up and run the OPC UA server."""
        # setup server
        self.server = Server()
        await self.server.init()
        
        self.server.set_endpoint(self.config.get('OPCSERVER','ep'))
        self.server.set_server_name(self.config.get('OPCSERVER','OpcServerName'))


        # setup our own namespace, not strictly necessary but should by spec
        uri = self.config.get('OPCSERVER','uri')
        self.uri = uri
        self.namespace = await self.server.register_namespace(uri)
        await self.add_variables_to_devices()

        self._logger.info('Starting OPC server!')
        async with self.server:
            # run forever and iterate over devices and publish their variables' values
            # at intervals defined by sleep time
            while self.should_run:
                try:
                    if self.sensors_updated:
                        #reset flag
                        self.sensors_updated = False
                        #update all devices with latest sensor dataframe value
                        for dev in self.devices:
                                await dev.publish_variables(self.sensors_df_reference)                            
                    
                    await asyncio.sleep(10)
                
                except Exception as e:
                    self._logger.error("Exception in publish loop..."+str(e))

    def main(self):
        """Main method to start the asyncio event loop and run the core server logic."""
        asyncio.run(self.core())

    def start(self):
        """Start the OPC UA server in a separate thread. Even though we are 
        using asyncio, we need to run it in its own thread to avoid blocking
        the main program execution."""
        #my_thread = threading.Thread(target=self.main, args=(self,))
        my_thread = threading.Thread(target=self.main)
        my_thread.start()
