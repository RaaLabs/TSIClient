# TSIClient - Get your data from TSI!

With TSIClient, you are able to get your data from Azure Time Series Insight! Just use this simple SDK :)

# Usage

## Setting API version (optional)

In the tsiAPI class, you can change the api version.
Initially, the api version is set to: "2018-11-01-preview"

## Prerequisite for usage (Step 1)

There are in total six parameters you have to provide to the constructor of the TSI class in order to create an instance, these are:

        self._apiVersion = "2018-11-01-preview"
        self._applicationName = applicationName
        self._enviromentName = enviroment
        self._client_id = client_id
        self._client_secret=client_secret
        self._tenant_id = tenant_id

As you can see, the API version is already set, but the rest has to be provided.
Here is an example of how you provide the variables:

```python
def getTSIkeysTags(vessel):
    tsiKeys = tsiApi(enviroment = 'ENVIRONMENT_NAME',
                     client_id = "CLIENT_ID",
                     client_secret = "CLIENT_SECRET",
                     applicationName = "APPLICATION_NAME",
                     tenant_id="TENANT_ID")

    Tags = [
            "/" + TAG_POWER.description
           ]
   
    TagsShort = [
            "/" + TAG_POWER.shortName
           ]
    
    return Tags, TagsShort , tsiKeys
```

After that, you can check if the authorization has been completed:

```python
def getAuth():
    try:
        tsiKeys
    except NameError:
        tsiKeys_exists = False
    else:
        tsiKeys_exists = True
        print("TSI Authorization completed")
```

## Getting data from TSI after authorization completed

### Creating Tag name for query

In order to query TSI with appropriate tags, you need to create a simple Tag class which contains two attributes:

1. Description from TSI tag
2. Shortname (whatever you want to shorten the description to)

Example:

```python
class Tag:
    def __init__(self,description,shortName):
        self.description = description
        self.shortName = shortName

# Tags to be feched
TAG_POWER = Tag("PropulsionAndSteeringArrangements/PropulsionEngine/Shaft/ShaftPower+(kW)","ME_ShaftPower")
```

After that, you need to concatinate the vessel name together with the long description that TSI expects.
This can be done with the following method:

```python
def addVesselNameToTags(tags, vessel):
    concatVariables = []
    for tag in tags:
        concatVariables.append(vessel+tag)
    
    if concatVariables:
        print("Tag-array has items")
    if not concatVariables:
        print("list is empty, you sure getVaribales() was successful?")
    return concatVariables
```

### Using getDataByDescription

In order to get data by description from TSI, you have to provide the following parameters to the method:

1. name of "vessel"
2. timespan (from time, to time)
3. interval
4. aggregate type
5. Time series Name

Example: 

```python
dataFrame = tsiKeys.getDataByDescription(addVesselNameToTags(Tags,vesselName),timespan=[timeFrom,timeTo],interval=intervalRequested,aggregate=aggType,TagsShort=TagsShort)
```
Now you should have all the data for the provided tags in a dataframe which is returned by the above function (getDataByDescription)


License
----

MIT License

Copyright (c) 2019 Anders Gill

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.


