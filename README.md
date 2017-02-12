d # EOS-Server
This is a python script to process data for the EOS weather system. It is normally run on the station (https://www.raspberrypi.org/) and will store all climate data into a MYSQL database (https://www.mysql.com/). Other systems/websites can access this data directly from the station database.

The service will also push weather data to various external sites like weatherunderground, pushover notifications and other weather reporting sites including our own remote cloud service to remotely archive the data.

This service also has capabilities to read (with pass through) and send NMEA standard (https://en.wikipedia.org/wiki/NMEA_0183) weather data. For use on ships and other mobile platforms.

Iot messaging data to a MQTT broker (http://mqtt.org/) site can also be published by this service so that details on any other device that supports MQTT protocol can be read directly from the stations weather output channel.

This script use other eos python packages (https://github.com/9thMile/EOS-Packages) as sub-routines in this service.
