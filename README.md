[![hacs_badge](https://img.shields.io/badge/HACS-Custom-orange.svg)](https://github.com/custom-components/hacs)

# Climate_IP - IP based climate device for Home Assistant
Implementation of ClimateDevice for controlling IP based AC units.
This component is able to work with any AC unit which can be controlled with REST API.
At this moment it is configured to work with:
* Samsung AC units available at port 8888 (new generation, REST API)
* Samsung AC units available at port 2878 (old generation, socket communication)
* Samsung MIM-H03 controller (REST API, port 8888)
* Samsung MIM-H04 controller via Smarthings cloud (REST API)

Support for any unit working with REST API can be easily added via YAML configuration file.

This component was created by SebuZet, he however appears to be MIA so I have forked and repaired his component
https://github.com/SebuZet/samsungrac

## Installation
1. Download all files from repo to newly created folder
2. move folder custom_components/climate_ip to your <ha_configuration_folder>
3. In __configuration.yaml__ file add section:
    * For new generation units (REST API, port 8888)
        ```
        - platform: climate_ip
          config_file: 'samsungrac.yaml'
          ip_address: 'device_ip'
          token: 'token'
          cert: 'ac14k_m.pem'
        ```
    * For MIM-H03 controller (REST API, port 8888)
        ```
        - platform: climate_ip
          config_file: 'mim-h03_heatpump.yaml'
          ip_address: 'device_ip'
          token: 'token'
          cert: 'ac14k_m.pem'
        ```
    * For old generation units:
        ```
        - platform: climate_ip
          config_file: 'samsung_2878.yaml'
          ip_address: 'device_ip'
          token: 'token'
          cert: 'ac14k_m.pem' #set as '' to skip certificate verification
          mac: 'AB:cd:EF:gh:IJ'
          poll: True
        ```
    * For Samsung EHS Mono Quiet heat pump with MIM-H04EN controller connected to Smartthings:
        ```
        # hot water controller
        - platform: climate_ip
          name: Samsung EHS DHW
          config_file: /config/custom_components/climate_ip/samsung_smartthings_dhw.yaml
          ip_address: api.smartthings.com
          device_id: aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee
          token: !secret smartthings_token
        # heating controller
        - platform: climate_ip
          name: Samsung EHS Heating
          config_file: /config/custom_components/climate_ip/samsung_smartthings_hvac.yaml
          ip_address: api.smartthings.com
          device_id: aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee
          token: !secret smartthings_token
        ```
## Configuration
1. Configuration parameters:

    | Parameter        | description           |  Required        |
    | ------------- |-------------|-------------|
    | config_file      | YAML configuration filename |Yes
    | ip_address      | Device IP address (e.g. 192.178.1.200) |Yes
    | token           | Access token gathered from the device        |Yes
    | cert_file   | certificate file name (default: ac14k_m.pem, Use __None__ to not use certification) | Usually Yes
    | mac      | MAC address of device | Only 2878 devices
    | name      | Device name (by default this value is taken from YAML config file) | No
    | controller    | Controller type to use (default, and the only one for now: yaml)  | No
    | poll      | Enable/disable state polling. Default: Taken from YAML config. Enabled for old gen devices | No
    | device_id | Set the device for controllers like MIM-H03, which can control multiple air con units | No. Strongly recommended for MIM-H03. Defaults to `032000000`
    | debug      | Enable/disable more debugs. Default: False | No
2. You need to have your device __token__. I will create a guide to gather it
3. If you're using MIM-H03, you may need your device ID, this can be had by running the following command and select your aircon device. It might be device `0`.
```sh
curl -k -H "Content-Type: application/json" -H "Authorization: Bearer __DEVICE_TOKEN__" --cert ac14k_m.pem -X GET https://__CLIMATE_IP_HOST__:8888/devices/
```

4. YAML configuration
You can easily add, remove or modify any device paramter to meet device capabilities.

## YAML configuration file syntax
```yaml
climate:
  - platform: climate_ip
    config_file: '/config/custom_components/climate_ip/samsung_2878.yaml'
    ip_address: 192.178.1.200
    token: SDADSAGFDfsdgdf234323
    mac: AA:BB:CC:DD:EE:FF
    name: 'AC Living Room'
    poll: True
```
## Functionality
Functionality depends on yaml configuration file and can be easily changed by editing those files. Currently configuration provides:
1. For new generation units (REST API, port 8888)
    * turn device on and off
    * sets and reads target/min/max temperatures
    * sets and reads swing direction
    * sets and reads fan level
    * sets and reads fan maximum level
    * sets and reads special mode (2Step, Comfort, Quiet etc)
    * sets and reads good sleep mode
    * turn purify mode on and off
    * turn auto clean mode on and off
    * turn beep mode on and off
    * read current indoor temperature
    * read device configuration
1. For old generation units 
    * turn device on and off
    * sets and reads target temperature
    * sets and reads swing direction (if supported)
    * sets and reads fan level (if supported)
    * sets and reads special mode (Comfort, Quiet etc)
    * turn purify mode on and off (if supported)
    * turn auto clean mode on and off (if supported)
    * read current indoor temperature
    * read device configuration
## Using
### Default functionality
This component implements Home Assistant ClimateDevice class. Functionality enabled in HA by default:
* turn device on/off
* select fan mode
* select swing mode
* select target temperatures (min, max and target)
### Device specific functions
Device specific functionality is added as extra service called **climate.climate_ip_set_property**.
Every device attribute can be set using this service with proper params.

```
{
  "entity_id" : "climate.salon_ac",
  "power" : "on",
  "purify" : "on",
  "special_mode" : "comfort"
}
```
### Switches for special functions
To make controllig device as easy as possible user can create template switches for operations defined as __Switch__ (please see configuration file). 
Below is an example of template switch for Purify option
```
switch:
  - platform: template
    switches:
      purify:
        value_template: "{{ is_state_attr('climate.salon_ac', 'purify', 'on') }}"
        turn_on:
          service: climate.climate_ip_set_property
          data:
            entity_id: climate.salon_ac
            purify: 'on'
        turn_off:
          service: climate.climate_ip_set_property
          data:
            entity_id: climate.salon_ac
            purify: 'off'
```
# References
 * [Samsung protocol description](https://community.openhab.org/t/newgen-samsung-ac-protocol/33805)
 * [HA forum](https://community.home-assistant.io/t/samsung-ac/11747/11)
 
## TODO
Documentation...


## Known issues 

SSL issues when running a pip install might be fixed with : 
https://github.com/atxbyea/samsungrac/issues/25


