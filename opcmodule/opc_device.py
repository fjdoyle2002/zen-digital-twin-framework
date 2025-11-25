import pandas as pd
import datetime as dt
from datetime import datetime, timezone
from asyncua import ua, Server
from asyncua.common.structures104 import new_enum
import logging

class OPCDevice:
    def __init__(self, device_name, device_description, device_class, variables_df):
        self.device_name = device_name
        self.description = device_description
        self.device_class = device_class
        self.variables_df = variables_df
        self.variables_map = {}
        self.variables_df['opc_var_ref'] = None
        self._logger = logging.getLogger(__name__)
        
        self.server = None
        self.namespace = None
        self.uri = None

        self.node = None
        self.variables = {}
        self.actuators = set()
        self.variable_types = {}       
            
    async def register_node(self):
        self.node = await self.server.nodes.objects.add_object(self.namespace, self.device_name)
        # <<< ADD THIS BLOCK >>>
        self._logger.info("=== JUST CREATED OBJECT DIAGNOSTICS ===")
        self._logger.info("Returned node type : %s", type(self.node))
        self._logger.info("Returned NodeId    : %s", self.node.nodeid)   # works on every version
        bn = await self.node.read_browse_name()
        self._logger.info("Returned BrowseName: %s (ns=%s)", bn.Name, bn.NamespaceIndex)
        self._logger.info("=======================================")
 
    
    def resolve_pandas_dtype_to_opc(self, data_type):
        #result is a tuple of UA datatype and initial value
        result = () 
        if data_type == 'float64':
            result = (ua.VariantType.Double, 0.0)
            
        elif data_type == 'int64':
            result = (ua.VariantType.Int64, 0)
            
        elif data_type == 'object':
            result = (ua.VariantType.String, " ")
        elif data_type == 'bool':
            result == (ua.VariantType.Boolean, False)
        else:
            #make String default
            result = (ua.VariantType.String, " ")
        
        self._logger.info("Resolved data type {} to OPC UA type {}".format(data_type, result[0]))
        return result
    
    def create_metadata_list(self, idx):
        meta_list = []
        #create subframe without device_name column, rather than explicitly listing all columns except unwanted ones,
        #as it will allow for further columns to be added to the variables dataframe without needing to update this code
        subframe_df = self.variables_df.drop(columns=['device_name', 'opc_var_ref'])
        for column in subframe_df:
            meta_list.append(str(subframe_df[column][idx]))
            
        return meta_list 
              

    async def add_variables(self, server, namespace, uri, module_tagmap):
        """
            server: asyncua Server object
            namespace: int, namespace index for this server 
            uri: str, namespace URI for this server
            module_tagmap: dict, mapping of tag names to OPC variable nodes at the module level (used to aggregate all device variables
            into a single map for easy access as calling code will not have device context)
        """
        self._logger.info("entered add variables with {}, {}, {} ".format(server, namespace, uri))
        
        self.server = server
        self.namespace = namespace
        self.uri = uri
        
        await self.register_node()

        self._logger.info("entering for loop in add variables")
        for idx in self.variables_df.index:
            tag_string = self.variables_df['var_name'][idx]
            tag_name = self.variables_df['tag_name'][idx]
            tag_desc = self.variables_df['description'][idx]
            val_type = self.variables_df['unit'][idx]
            #property_name = self.tag_metadata['property_name'][idx]
            #curr_dtype = self.data.dtypes[tag_string]
            curr_dtype = self.variables_df['data_type'][idx]
            self._logger.info("Processing variable: {} with data type: {}".format(tag_string, curr_dtype))
            res_dtype = self.resolve_pandas_dtype_to_opc(curr_dtype)
            curr_meta_list = self.create_metadata_list(idx)
            #curr_meta_enum = ua.uatypes.Enum("metadata",curr_meta_list)
                    
            self._logger.info("CREATING VARIABLE: "+tag_string)
            #curr_var = await self.node.add_variable(("ns=2;s='{}'".format(tag_string)), tag_desc, res_dtype[1], varianttype=res_dtype[0])
            #curr_var = await self.node.add_variable(self.namespace, tag_string, res_dtype[1], varianttype=res_dtype[0])
            curr_node_id_str = 'ns=2;s={}'.format(tag_name)
            self._logger.info("Curr Node ID String: {}".format(curr_node_id_str)) 
            curr_node_id = ua.NodeId.from_string(curr_node_id_str)
            self._logger.info("Curr Node ID: {}".format(curr_node_id))    
            curr_var = await self.node.add_variable(curr_node_id, tag_desc, res_dtype[1], varianttype=res_dtype[0])
            #if this is an actuator, make it writeable and make sure we don't overwrite it with DT data
            if self.variables_df['ep_type'][idx] == 'actuator':
                self.actuators.add(tag_name)
                await curr_var.set_writable(True)
            self.variables_map[tag_name] = curr_var
            module_tagmap[tag_name] = curr_var

            #now, let's attach the metadata to this variable
            enode = await new_enum(self.server, self.namespace, "MetaEnum", curr_meta_list)
            custom_objs = await server.load_data_type_definitions()                 
            await curr_var.add_variable(self.namespace, "meta_enum", 0, datatype=enode.nodeid)
            #print('Display Name:{},Description{}'.format(property_name, val_type))
            
            display_dv = ua.DataValue(ua.LocalizedText(tag_string))
            await curr_var.write_attribute(ua.AttributeIds.DisplayName,display_dv)
            desc_dv = ua.DataValue(ua.LocalizedText(str(val_type)))
            await curr_var.write_attribute(ua.AttributeIds.Description, desc_dv)

            self.variables[tag_name] = curr_var
            self.variable_types[tag_name] = res_dtype[0]


              
    async def publish_variables(self, sensors_df):
         """sensors_df is a dataframe containing the latest sensor values for all tags in this device
         and others as well; we will filter to only those relevant to this device"""
         
         timestamp = datetime.now(timezone.utc)
         
         for curr_tag in self.variables:
              #we need to make sure that data from the digital twin doesn't overwite actuator values
              #set by external users. As sensors and actuators may have identical naming conventions, 
              #it is easiest to ensure thise doesn't happen by checking intent in the opc_variables settings
              if curr_tag not in self.actuators:
                curr_variable = self.variables[curr_tag]
                curr_value = sensors_df.loc[sensors_df['opc_tag_name'] == curr_tag, 'current_val'].values[0]
                self._logger.info("Publishing variable {} with value {}".format(curr_tag, curr_value))
                curr_ua_type = self.variable_types[curr_tag]
                #cast the value as appropriate type
                #print("CURR_VALUE: {}, CURR_UA_TYPE: {}".format(curr_value, curr_ua_type))
                curr_ua_dvalue = ua.DataValue(ua.Variant(curr_value, curr_ua_type), ServerTimestamp=timestamp, SourceTimestamp=timestamp)              
                
                #now write the variable value  
                await curr_variable.write_value(curr_ua_dvalue)
              
              

              

              
              
              
         
         
         

