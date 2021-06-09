# AIS NMEA NiFi Custom Processor

## Input string considerations

This NiFi custom processor take two kind of extended AIS NMEA message

- **Historical**: where the last string date info is expressed following date format `yyyyMMddHHmmssSSS`
> !ABVDM,1,1,6,B,13f?Kt001tOK:OHDk<a;Ba3l0400,0*31,20200101000000291

Above AIS date value corresponds to `2020-01-01 00:00:00.291 UTC`
- **Live**: where the last date info is expressed in seconds UTC time

> !ABVDM,1,1,4,A,13sNv<7P?w<tSF0l4Q@>4?wp0t05,0*14,1614615950

Above AIS date value corresponds to `2021-03-01 16:25:50 UTC`

## NiFi Processor Properties

In order to distinguish two mentioned cases, there are two mandatory properties for this custom processor:
- _Time in UTC seconds_ - `true` or `false`, true if is like **Live** data
- _Time Pattern_ - specify the rule like **Historical** data, like `yyyyMMddHHmmssSSS`

## How to deploy Custom NiFi Processor

To deploy the custom processor in NiFi, we will place into terminal inside our processor project directory and build the project with these commands (maven installed is **required**):
```sh
$ cd ais
$ mvn clean install
```
Output of the build command will end as follows:
```sh
[INFO] ------------------------------------------------------------------------
[INFO] Reactor Summary for ais 1.0-SNAPSHOT:
[INFO] 
[INFO] ais ................................................ SUCCESS [  1.179 s]
[INFO] nifi-ais-processors ................................ SUCCESS [  2.103 s]
[INFO] nifi-ais-nar ....................................... SUCCESS [  0.262 s]
[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
[INFO] Total time:  4.659 s
[INFO] Finished at: 2021-03-04T12:55:26+01:00
[INFO] ------------------------------------------------------------------------
```
Now, copy the build _"nifi-ais-nar-1.0-SNAPSHOT.nar"_ file to NiFi _"lib"_ directory and restart nifi:
```
$ cp nifi-ais-nar/target/nifi-ais-nar-1.0-SNAPSHOT.nar NIFI_HOME/lib/
$ NIFI_HOME/bin/nifi.sh run
```
Open a browser and navigate to the nifi UI at _"http://localhost:8080/nifi"_ and then you may found the created processor.

## Resource

This readme was created following [this guide on Medium](https://medium.com/@g22shubham/apache-nifi-part-i-create-custom-processor-675fcf251a1).
