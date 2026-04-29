# WinCC Unified Open Pipe — Programming Reference

> Source: SIMATIC HMI / WinCC Unified — *WinCC Unified Open Pipe*, Operating Manual, 11/2023 (en-US).
> This file is a structured reference for an LLM that programs against the Open Pipe interface.

---

## 1. Overview

WinCC Unified Open Pipe is an Openness mechanism, based on **named-pipe** technology, that lets a client application connect to **WinCC Unified Runtime (RT)** and exchange data via tags and alarms.

Compared with Openness RT (ODK) it offers a **reduced** command set, but the client side may be written in **any language that supports pipes** (incl. plain CMD/batch, Python, Node.js, PowerShell, etc.).

Two syntaxes are provided in parallel on the same pipe:

| Syntax       | Format                       | Use case                                                       | Cookies      |
|--------------|------------------------------|----------------------------------------------------------------|--------------|
| Basic syntax | Plain text, space-separated   | Single-object operations from shell scripts / batch            | Not used     |
| Expert syntax| One-line JSON                 | Multi-object operations, scripts/programming with JSON parser  | Mandatory    |

Sample code is shipped on the installation medium under
`Support\Openness\Siemens.Unified.Openness_SDK_<version>.zip` → subfolder `OpenPipe\Samples`.

---

## 2. Connecting to the Pipe

### Pipe name

| OS      | Pipe path                  |
|---------|----------------------------|
| Windows | `\\.\pipe\HmiRuntime`      |
| Linux   | `/tmp/HmiRuntime`          |

The pipe is a FIFO data stream between two processes:

* **Server** — `OpennessManager` inside WinCC Unified RT creates the pipe and processes requests.
* **Client** — your application connects to the pipe by name, sends requests, reads responses on the **same pipe instance**.

### Framing

* Once the pipe is open, send **single-line commands** terminated with a line break (`\n` or `\r\n`).
* Responses are returned on the same pipe instance.

### Security / required user group

Open Pipe is restricted to **local communication** between the client app and Runtime. The OS user running the client script must be a member of:

| OS      | Required group  |
|---------|-----------------|
| Windows | `SIMATIC HMI`   |
| Linux   | `industrial`    |

---

## 3. Browse behavior (common to all `Browse*` commands)

* **Initial request** — define the query via optional parameters.
* **Page size** — optional parameter in the initial request. If the result count exceeds the page size, the response is paged. If the parameter is omitted, the configured `DefaultPageSize` is used.
* **Next request** — fetches the next page; only carries the `Next` parameter (and, in expert syntax, the `ClientCookie`).
* A browse command is **complete** when a Next request returns an empty response.
* A browse is **canceled** if:
  * The configured inactivity time limit (`BrowseTimeOut`) is exceeded, **or**
  * A new browse request is started.
* `BrowseAlarmClasses` does **not** support paging.

### Configurable settings (read with `ReadConfig`, set with `WriteConfig`)

| Setting           | Default | Range            | Description                                                                                                  |
|-------------------|---------|------------------|--------------------------------------------------------------------------------------------------------------|
| `DefaultPageSize` | 1000    | `0 … Uint32_Max` | Page size used when `PageSize` is not specified. `0` = no pagination.                                        |
| `BrowseTimeOut`   | 300 s   | `0 … Uint32_Max` | Seconds of inactivity after which a paged browse is canceled (a subsequent `Next` returns an error). `0` = no timeout. |

Changes apply to the **current pipe session** only.

---

## 4. Basic Syntax

### 4.1 Characteristics

* Plain, text-based — no JSON.
* Operates on **single objects** only (e.g. write one tag).
* Object names must **not contain special characters or spaces**.
* **No cookies.**

### 4.2 Request / response shape

```
Request:    <Command> <Object> <Value>
Success:    Notify<Command> <Object> <Value1>...<ValueN>
Error:      Error<Command>  <Object> <Error text>
```

### 4.3 Command summary

| Command                                              | OnSuccess                                                                     | OnError                                                                              | Description                                                                 |
|------------------------------------------------------|-------------------------------------------------------------------------------|--------------------------------------------------------------------------------------|-----------------------------------------------------------------------------|
| `SubscribeTagValue <Tag>`                            | `NotifySubscribeTagValue <Tag> <Quality> <Value>`                             | `ErrorSubscribeTagValue <Tag> <Error>` / `ErrorNotifyTagValue <Tag> <Error>`         | Subscribe a tag for monitoring. `Quality ∈ {Good, Bad, Uncertain}`.        |
| `UnsubscribeTagValue <Tag>`                          | `NotifyUnsubscribeTagValue <Tag>`                                             | `ErrorUnsubscribeTagValue <Tag> <Error>`                                             | Unsubscribe.                                                                |
| `ReadTagValue <Tag>`                                 | `NotifyReadTagValue <Tag> <Quality> <Value>`                                  | `ErrorReadTagValue <Tag> <Error>`                                                    | Read tag value + quality.                                                   |
| `WriteTagValue <Tag> <Value>`                        | `NotifyWriteTagValue <Tag>`                                                   | `ErrorWriteTagValue <Tag> <Error>`                                                   | Write a single tag.                                                         |
| `SetCharSet <Value>`                                 | `NotifySetCharSet <Value>`                                                    | `ErrorSetCharSet <Value> <Error>`                                                    | Change character encoding.                                                  |
| `ReadConfig <Param>`                                 | `NotifyReadConfig <Param> <Value>`                                            | `ErrorReadConfig <Error>`                                                            | Read a browse setting.                                                      |
| `WriteConfig <Param> <Value>`                        | `NotifyWriteConfig <Param>`                                                   | `ErrorWriteConfig <Error>`                                                           | Set a browse setting.                                                       |
| `BrowseTags …`                                       | `NotifyBrowseTags <System>::<Tag> …`                                          | `ErrorBrowseTags <Error>`                                                            | List tags.                                                                  |
| `BrowseConfiguredAlarms …`                           | `NotifyBrowseConfiguredAlarms <System>::<Alarm> …`                            | `ErrorBrowseConfiguredAlarms <Error>`                                                | List configured alarms.                                                     |
| `BrowseAlarmClasses [<System>]`                      | `NotifyBrowseAlarmClasses <System>::<AlarmClass> …`                           | `ErrorBrowseConfiguredAlarms <Error>` *(sic — per manual)*                           | List alarm classes (no paging).                                             |

### 4.4 Commands — detail

#### `SubscribeTagValue`

Subscribe the named tag for monitoring.

* If the tag value contains a line break (`\n`), the value cannot be signaled — an error is returned.
* Re-subscribing the same tag generates an error.
* A **global** subscription error is signaled with `ErrorSubscribeTagValue`. Because no monitoring was set up, no unsubscribe is required.
* A **value-level** error is signaled with `ErrorNotifyTagValue`. Monitoring **is** set up; the value can't be signaled. Unsubscribe when no longer needed.

```
Request:  SubscribeTagValue Tag_1
Success:  NotifySubscribeTagValue Tag_1 Good 10
          NotifySubscribeTagValue Tag_1 Uncertain 0
          NotifySubscribeTagValue Tag_1 Bad 12
Error:    ErrorSubscribeTagValue Tag_1 Tag does not exist
          ErrorSubscribeTagValue Tag_1 Subscription already exists
          ErrorNotifyTagValue   Tag_1 Encoding error
          ErrorNotifyTagValue   Tag_1 Value contains newline
```

#### `UnsubscribeTagValue`

```
Request:  UnsubscribeTagValue Tag_1
Success:  NotifyUnsubscribeTagValue Tag_1
Error:    ErrorUnsubscribeTagValue Tag_1 Subscription does not exist
```

#### `ReadTagValue`

Read tag value + quality from the system. If the value contains `\n`, an error is returned.

```
Request:  ReadTagValue Tag_1
Success:  NotifyReadTagValue Tag_1 Good 10
Error:    ErrorReadTagValue Tag_1 Tag does not exist
          ErrorReadTagValue Tag_1 Encoding error
          ErrorReadTagValue Tag_1 Value contains newline
```

#### `WriteTagValue`

Write to a single tag. If the supplied value contains `\n`, **only the substring before the line break** is written.

```
Request:  WriteTagValue Motor.Label MC001
Success:  NotifyWriteTagValue Motor.Label
Error:    ErrorWriteTagValue Motor.Label Tag does not exist
```

#### `SetCharSet`

Sets the character encoding used on the pipe to one of: `UTF-8`, `cp437`, `cp850`. Default is **UTF-8**.

* `cp850` is the typical SystemLocale on German Windows.
* `cp437` is the typical SystemLocale on US Windows.
* Internally strings are Unicode (often UTF-16); on the pipe they are converted to bytes per the active character set. If a character cannot be encoded (e.g. Greek "π" in cp437) an `encoding error` is signaled.

```
Request:  SetCharSet UTF-8
Success:  NotifySetCharSet UTF-8
Error:    ErrorSetCharSet UTF-9 unknown character set
```

#### `ReadConfig`

Read **one** browse setting per call.

```
Param ∈ { DefaultPageSize, BrowseTimeOut }

Request:  ReadConfig DefaultPageSize
Success:  NotifyReadConfig DefaultPageSize 1000
          NotifyReadConfig BrowseTimeOut  300
Error:    ErrorReadConfig Invalid arguments passed to browsing function.
```

#### `WriteConfig`

Set **one** browse setting per call.

```
Request:  WriteConfig DefaultPageSize 500
Success:  NotifyWriteConfig DefaultPageSize
Error:    ErrorWriteConfig Invalid arguments passed to browsing function.
```

#### `BrowseTags`

List tag names of the local HMI system or of all systems linked via Runtime Collaboration.

```
Initial:  BrowseTags <System> <PageSize> --filter <Filter>
Next:     BrowseTags --next
```

* `System` *(optional)* — `*` for all collaborating systems, omitted = local system.
* `PageSize` *(optional)* — default = configured page size.
* `--filter <Filter>` *(optional)* — restrict by tag `Name`. Wildcards: `*` (0+ chars), `?` (1 char). Default = all tags of the selected system(s).

```
Initial example:  BrowseTags * 100 --filter *Tag0*
Success:          NotifyBrowseTags HMI_RT_1::InternalTag0 HMI_RT_2::InternalTag01 HMI_RT_2::InternalTag02
Error:            ErrorBrowseTags Invalid arguments passed to browsing function.
```

#### `BrowseConfiguredAlarms`

List names of configured alarms of the local system or all collaborating systems.

```
Initial:  BrowseConfiguredAlarms <System> <PageSize> --filter <Filter>
Next:     BrowseConfiguredAlarms --next
```

Parameters and wildcards: same scheme as `BrowseTags`. Filter applies to the alarm `Name`.

```
Initial example:  BrowseConfiguredAlarms * 100 --filter *Analog*
Success:          NotifyBrowseConfiguredAlarms HMI_RT_1::Motor_1:AnalogAlarm_1 HMI_RT_2::Motor_2:AnalogAlarm_1
Error:            ErrorBrowseConfiguredAlarms A parameter is not valid or out of range.
```

> *(The manual shows the success keyword as `NotifyBBrowseConfiguredAlarms` in one place — that's a documentation typo; the correct token is `NotifyBrowseConfiguredAlarms`.)*

#### `BrowseAlarmClasses`

List alarm classes of the local system or all collaborating systems.
**Paging is not available for this command.**

```
Request:  BrowseAlarmClasses [<System>]      # System: '*' = all collab systems, default = local
Success:  NotifyBrowseAlarmClasses HMI_RT_1::Alarm HMI_RT_1::SystemNotification HMI_RT_1::SystemInformation
                                  HMI_RT_1::SystemAlarm HMI_RT_1::Notification HMI_RT_1::OperatorInputInformation
Error:    ErrorBrowseAlarmClasses Invalid arguments passed to browsing function.
```

### 4.5 Reference — tag properties returned by `ReadTagValue`

All values are transferred as strings.

| Property            | Description                                                                  |
|---------------------|------------------------------------------------------------------------------|
| `Name`              | Name of the tag.                                                             |
| `Value`             | Tag value at the moment of the read.                                         |
| `Quality`           | One of `Good`, `Bad`, `Uncertain`.                                           |
| `ErrorDescription`  | Description of the error code of the last read/write operation of the tag.   |

---

## 5. Expert Syntax

### 5.1 Characteristics

* Full **JSON** request and response, one JSON object per line.
* Supports **single and bulk** operations (multiple tags / alarms in one call).
* Character coding is **always UTF-8**.
* `ClientCookie` is **mandatory** — used for correlation and to drive `Unsubscribe*` and paging `Next` requests.

### 5.2 Request / response shape

```jsonc
// Request
{
  "Message": "<Command>",
  "Params": {
    "<ObjectName>": [ "<Param1>", "<Param2>" ]
  },
  "ClientCookie": "<Cookie>"
}

// Success
{
  "Message": "Notify<Command>",
  "Params": {
    "<ObjectName>": [ {"<Value1>"}, {"<Value2>"} ]
  },
  "ClientCookie": "<Cookie>"
}

// Error
{
  "Message": "Error<Command>",
  "ErrorCode": "<Code>",
  "ErrorDescription": "<Text>",
  "ClientCookie": "<Cookie>"
}
```

### 5.3 Command summary

| Command                  | OnSuccess fields                                                                                                             | OnError                          | Description                                                                                                                   |
|--------------------------|------------------------------------------------------------------------------------------------------------------------------|----------------------------------|-------------------------------------------------------------------------------------------------------------------------------|
| `SubscribeTag`           | `NotifySubscribeTag` → `Name, Value, TimeStamp, Quality, QualityCode, hasChanged, ErrorCode, ErrorDescription, ClientCookie` | `ErrorSubscribeTag`              | Subscribe one or more tags for monitoring.                                                                                    |
| `UnsubscribeTag`         | `NotifyUnsubscribeTag` (cookie only)                                                                                         | `ErrorUnsubscribeTag`            | Unsubscribe the cookie's subscription.                                                                                        |
| `ReadTag`                | `NotifyReadTag` (per-tag: `Name, Value, TimeStamp, Quality, QualityCode, ErrorCode, ErrorDescription`)                       | `ErrorReadTag`                   | Read multiple tags.                                                                                                           |
| `WriteTag`               | `NotifyWriteTag` (per-tag: `Name, ErrorCode, ErrorDescription`)                                                              | `ErrorWriteTag`                  | Write multiple tag values.                                                                                                    |
| `SubscribeAlarm`         | `NotifySubscribeAlarm` (`Alarms: [...]`)                                                                                     | `ErrorSubscribeAlarm`            | Subscribe alarms by filter / system / language for monitoring.                                                                |
| `UnsubscribeAlarm`       | `NotifyUnsubscribeAlarm`                                                                                                     | `ErrorUnsubscribeAlarm`          | Unsubscribe alarms.                                                                                                           |
| `ReadAlarm`              | `NotifyReadAlarm` (`Alarms: [...]`)                                                                                          | `ErrorReadAlarm`                 | Read all currently active alarms matching filter / system / language.                                                         |
| `ReadConfig`             | `NotifyReadConfig` (`{Parameter: Value, ...}`)                                                                               | `ErrorReadConfig`                | Read browse settings (multiple per call).                                                                                     |
| `WriteConfig`            | `NotifyWriteConfig` (`{Parameter: Value, ...}`)                                                                              | `ErrorWriteConfig`               | Set browse settings (multiple per call).                                                                                      |
| `BrowseTags`             | `NotifyBrowseTags` (`Tags: [{Name, DisplayName, DataType, ...}]`)                                                            | `ErrorBrowseTags`                | List tags; supports paging via `Next`.                                                                                        |
| `BrowseConfiguredAlarms` | `NotifyBrowseConfiguredAlarms` (`AlarmClasses: [{Name, Alarms: [...]}]`)                                                     | `ErrorBrowseConfiguredAlarms`    | List configured alarms; supports paging via `Next`.                                                                           |
| `BrowseAlarmClasses`     | `NotifyBrowseAlarmClasses` (`AlarmClasses: [...]`)                                                                           | `ErrorBrowseAlarmClasses`        | List alarm classes (no paging).                                                                                               |

> Detailed alarm/tag attribute names are documented in *Runtime — Open Development Kit (ODK)*.

### 5.4 Commands — detail

#### `SubscribeTag`

Subscribes one or more tags. Monitored properties: `Value`, `Quality`, `QualityCode`, `TimeStamp`.
Every notification returns **all** monitored tags, even if only one changed (value, quality code, or timestamp).
The same tag may be monitored under multiple subscriptions.

```jsonc
// Request
{"Message":"SubscribeTag","Params":{"Tags":["Tag_0","Tag_1"]},"ClientCookie":"mySubscription1"}

// Success
{"Message":"NotifySubscribeTag","Params":{"Tags":[
  {"Name":"Tag_0","Quality":"Good","QualityCode":"192","TimeStamp":"2019-01-30T11:25:35Z","Value":"16","ErrorCode":0,"ErrorDescription":""},
  {"Name":"Tag_1","Quality":"Uncertain","QualityCode":"76","TimeStamp":"2019-01-30T11:25:35Z","Value":"1","ErrorCode":-2147483620,"ErrorDescription":"Tag does not exist"}
]},"ClientCookie":"mySubscription1"}

// Error
{"Message":"ErrorSubscribeTag","ErrorCode":-2147483621,"ErrorDescription":"Subscription could not be created","ClientCookie":"mySubscription1"}
```

#### `UnsubscribeTag`

Cancels the subscription identified by `ClientCookie`.

```jsonc
{"Message":"UnsubscribeTag","ClientCookie":"mySubscription1"}
{"Message":"NotifyUnsubscribeTag","ClientCookie":"mySubscription1"}
{"Message":"ErrorUnsubscribeTag","ErrorCode":-2147483621,"ErrorDescription":"Subscription could not be closed","ClientCookie":"mySubscription1"}
```

#### `ReadTag`

Read multiple tags. The order of tags in the response matches the request order.

```jsonc
{"Message":"ReadTag","Params":{"Tags":["Tag_0","Tag_1"]},"ClientCookie":"myRequest1"}

{"Message":"NotifyReadTag","Params":{"Tags":[
  {"Name":"Tag_0","Quality":"Good","QualityCode":"192","TimeStamp":"2019-01-30T11:25:35Z","Value":"16","ErrorCode":0,"ErrorDescription":""},
  {"Name":"Tag_1","Quality":"Uncertain","QualityCode":"76","TimeStamp":"2019-01-30T11:25:35Z","Value":"1","ErrorCode":-2147483620,"ErrorDescription":"Tag does not exist"}
]},"ClientCookie":"myRequest1"}

{"Message":"ErrorReadTag","ErrorCode":-2147483621,"ErrorDescription":"Failed to Read","ClientCookie":"myRequest1"}
```

#### `WriteTag`

Write multiple tag values.

```jsonc
{"Message":"WriteTag","Params":{"Tags":[
  {"Name":"Tag_0","Value":"50"},
  {"Name":"Tag_1","Value":"40"}
]},"ClientCookie":"myRequest2"}

{"Message":"NotifyWriteTag","Params":{"Tags":[
  {"Name":"Tag_0","ErrorCode":0,"ErrorDescription":""},
  {"Name":"Tag_1","ErrorCode":-2147483620,"ErrorDescription":"Tag does not exist"}
]},"ClientCookie":"myRequest2"}

{"Message":"ErrorWriteTag","ErrorCode":-2147483621,"ErrorDescription":"Failed to Write","ClientCookie":"myRequest2"}
```

#### `SubscribeAlarm`

Subscribe to changes on active alarms. The first `NotifySubscribeAlarm` returns **all currently active** alarms; subsequent notifications fire on **state changes** only.

Parameters:

* `SystemNames` *(optional)* — list of systems; empty/missing = all known systems.
* `Filter` *(optional)* — alarm filter expression (see §6).
* `LanguageId` *(optional)* — locale ID for textual fields.
* `ClientCookie` — required, used to correlate notifications and to call `UnsubscribeAlarm`.

```jsonc
{
  "Message":"SubscribeAlarm",
  "Params":{
    "SystemNames":["System0","System1"],
    "Filter":"AlarmClassName != 'Warning'",
    "LanguageId":1033
  },
  "ClientCookie":"CookieForSubscribeAlarms123"
}
```

Example success notification (abbreviated; full set of alarm properties listed in §7):

```jsonc
{"Message":"NotifySubscribeAlarm","ClientCookie":"CookieForSubscribeAlarms123","params":{"Alarms":[
  {
    "Name":"RUNTIME_1::Tag_2:Alarm2","AlarmClassName":"Alarm","AlarmClassSymbol":"Alarm",
    "AlarmText1":"","AlarmText2":"", "...":"...",
    "State":"1","StateMachine":"7","StateText":"R","SuppressionState":"0",
    "Priority":"1","Tag":"RUNTIME_1::Tag_2","Value":"7","ValueQuality":"192",
    "RaiseTime":"2019-01-30 11:25:39.9780320","ModificationTime":"2019-01-30 11:25:39.9780320",
    "AcknowledgmentTime":"1970-01-01 00:00:00.0000000","ClearTime":"1970-01-01 00:00:00.0000000","ResetTime":"1970-01-01 00:00:00.0000000",
    "Connection":"1.0.0.0.0.0","HostName":"md1z5cpc","ID":"0","InstanceID":"9",
    "ChangeReason":"3","NotificationReason":"1","Origin":"","Area":"","SourceID":"","SourceType":"1",
    "BackColor":"4294967295","TextColor":"4278190080","Flashing":"FALSE",
    "DeadBand":"No deadband configured.","ValueLimit":"No limit configured.","Duration":"00:00:01.7431098",
    "AlarmGroupID":"1"
  }
]}}
```

Error example:

```jsonc
{"Message":"ErrorSubscribeAlarm","ErrorCode":"-2147483621","ErrorDescription":"Alarm Subscription failed because of invalid filter","ClientCookie":"CookieForSubscribeAlarms123"}
```

#### `UnsubscribeAlarm`

```jsonc
{"Message":"UnsubscribeAlarm","ClientCookie":"CookieForSubscribeAlarms123"}
{"Message":"NotifyUnsubscribeAlarm","ClientCookie":"CookieForSubscribeAlarms123"}
{"Message":"ErrorUnsubscribeAlarm","ErrorCode":-2147483621,"ErrorDescription":"Subscription could not be closed","ClientCookie":"CookieForSubscribeAlarms123"}
```

#### `ReadAlarm`

Reads all alarms currently active. Same parameter set as `SubscribeAlarm` (`SystemNames`, `Filter`, `LanguageId`, `ClientCookie`).

```jsonc
{
  "Message":"ReadAlarm",
  "Params":{"SystemNames":["System0","System1"],"Filter":"","LanguageId":1033},
  "ClientCookie":"CookieForReadAlarmRequest456"
}

// Success: same alarm property structure as NotifySubscribeAlarm.
{"Message":"NotifyReadAlarm","ClientCookie":"CookieForReadAlarmRequest456","params":{"Alarms":[ /* ... */ ]}}

// Error
{"Message":"ErrorReadAlarm","ErrorCode":-2147483621,"ErrorDescription":"Alarm Subscription failed because of invalid filter","ClientCookie":"CookieForReadAlarmRequest456"}
```

#### `ReadConfig` (expert)

`Params` is a list of parameter names. Possible values: `DefaultPageSize`, `BrowseTimeOut`.

```jsonc
{"Message":"ReadConfig","Params":["DefaultPageSize","BrowseTimout"],"ClientCookie":"myBrowseAlarmsRequest1"}

{"Message":"NotifyReadConfig","Params":{"DefaultPageSize":500,"BrowseTimeOut":60},"ClientCookie":"myBrowseAlarmsRequest1"}

{"Message":"ErrorReadConfig","ErrorCode":"-2165322729","ErrorDescription":"Invalid arguments passed to browsing function.","ClientCookie":"myBrowseAlarmsRequest1"}
```

#### `WriteConfig` (expert)

`Params` carries name/value pairs. Defaults: `DefaultPageSize=1000`, `BrowseTimeOut=300`.

```jsonc
{"Message":"WriteConfig","Params":["DefaultPageSize":500,"BrowseTimout":60],"ClientCookie":"myBrowseAlarmsRequest1"}

{"Message":"NotifyWriteConfig","Params":{"DefaultPageSize":500,"BrowseTimeOut":60},"ClientCookie":"myBrowseAlarmsRequest1"}

{"Message":"ErrorWriteConfig","ErrorCode":"-2165322733","ErrorDescription":"A parameter is not valid or out of range.","ClientCookie":"myBrowseAlarmsRequest1"}
```

> Note: the manual prints the example `Params` as a JSON array using `:` separators; in practice clients should use a proper JSON object `{ "DefaultPageSize": 500, "BrowseTimeOut": 60 }` consistent with the `NotifyWriteConfig` shape.

#### `BrowseTags` (expert)

Returns at minimum `Name`, `DisplayName`, `DataType`; additional attributes can be requested.

Initial request:

```jsonc
{
  "Message":"BrowseTags",
  "Params":{
    "LanguageId":1033,
    "Filter":"*InternalTag_Bool_1*",
    "Attributes":["AcquisitionMode","MaxValue"],
    "PageSize":50,
    "SystemNames":["HMI_RT_1","HMI_RT_2"]
  },
  "ClientCookie":"myBrowseTagRequest1"
}
```

Parameters:

| Param          | Optional? | Notes                                                                                                                                                                                                                                                                                                                |
|----------------|-----------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `LanguageId`   | Optional  | Locale for `DisplayName`. Default: the default language of the source system.                                                                                                                                                                                                                                       |
| `Filter`       | Optional  | Wildcard match against `Name`. `*` = 0+ chars, `?` = 1 char. Default = all tags.                                                                                                                                                                                                                                     |
| `Attributes`   | Optional  | List of extra tag attributes. The response always includes `Name`, `DisplayName`, `DataType`. `"*"` returns the full TIA Portal supported set: `Name, DisplayName, AcquisitionMode, Persistent, DataType, Connection, AcquisitionCycle, MaxLength, SubstituteValueUsage, InitialValue, SubstituteValue, InitialMaxValue, InitialMinValue, Address`. |
| `PageSize`     | Optional  | Default = configured page size.                                                                                                                                                                                                                                                                                      |
| `SystemNames`  | Optional  | `"*"` = all collaborating systems; comma-separated list otherwise. Default = local system.                                                                                                                                                                                                                           |
| `ClientCookie` | Required  | Response cookie name.                                                                                                                                                                                                                                                                                                 |

Next request:

```jsonc
{"Message":"BrowseTags","Params":"Next","ClientCookie":"myBrowseTagRequest1"}
```

Success / error:

```jsonc
{"ClientCookie":"<myBrowseTagRequest1>","Message":"NotifyBrowseTags","Params":{"Tags":[
  {"AcquisitionMode":0,"MaxValue":1000,"DataType":1,"DisplayName":"HMI_RT_1::InternalTag_Bool_1","Name":"HMI_RT_1::InternalTag_Bool_1"}
]}}

{"Message":"ErrorBrowseTags","ErrorCode":"-2165323798","ErrorDescription":"Invalid system name.","ClientCookie":"myBrowseTagRequest1"}
```

#### `BrowseConfiguredAlarms` (expert)

Returns at minimum `AlarmClass`, `Name`, `Area`; additional attributes can be requested.

Initial request:

```jsonc
{
  "Message":"BrowseConfiguredAlarms",
  "Params":{
    "LanguageId":1033,
    "Filter":"*alarm_*",
    "Attributes":["Priority"],
    "PageSize":50,
    "SystemNames":["HMI_RT_1"]
  },
  "ClientCookie":"myBrowseAlarmsRequest1"
}
```

Parameters:

| Param          | Optional? | Notes                                                                                                                                                                                                                                                       |
|----------------|-----------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `LanguageId`   | Optional  | Locale for alarm texts. Default: source system default.                                                                                                                                                                                                     |
| `Filter`       | Optional  | Wildcard match against `Name`. `*`, `?` as for `BrowseTags`.                                                                                                                                                                                                 |
| `Attributes`   | Optional  | Extra attributes. Response always contains `AlarmClass`, `Name`, `Area`. `"*"` returns: `Name, ID, SourceType, AlarmClassName, Priority, EventText, AlarmText1..AlarmText9, InfoText, Group, Origin, Area`.                                                  |
| `PageSize`     | Optional  | Default = configured page size.                                                                                                                                                                                                                              |
| `SystemNames`  | Optional  | `"*"` or list. Default = local system.                                                                                                                                                                                                                       |
| `ClientCookie` | Required  | Response cookie name.                                                                                                                                                                                                                                        |

Next request:

```jsonc
{"Message":"BrowseConfiguredAlarms","Params":"Next","ClientCookie":"myBrowseAlarmsRequest1"}
```

Success / error:

```jsonc
{
  "ClientCookie":"myBrowseAlarmsRequest1",
  "Message":"NotifyBrowseConfiguredAlarms",
  "Params":{"AlarmClasses":[
    {
      "Name":"HMI_RT_1::Warning",
      "Alarms":[
        {"AlarmClassName":"HMI_RT_1::Warning","Area":"HMI_RT_1::Alarming","Name":"HMI_RT_1::Tag6:Analog_alarm_2","Priority":12},
        {"AlarmClassName":"HMI_RT_1::Warning","Area":"HMI_RT_1::Alarming","Name":"HMI_RT_1::AlarmTag_1:Discrete_alarm_1","Priority":12}
      ]
    }
  ]}
}

{"Message":"ErrorBrowseConfiguredAlarms","ErrorCode":"-2165323798 / -2165322773","ErrorDescription":"Invalid system name. or Your browse request has been expired","ClientCookie":"myBrowseAlarmsRequest1"}
```

#### `BrowseAlarmClasses` (expert)

Returns alarm classes; **no paging**.

Request:

```jsonc
{
  "Message":"BrowseAlarmClasses",
  "Params":{
    "Filter":"*Alarm*",
    "Attributes":["..."],
    "SystemNames":["HMI_RT_1"]
  },
  "ClientCookie":"myBrowseAlarmClassRequest1"
}
```

Parameters:

| Param          | Optional? | Notes                                                                                                                                                                                                                                                                                                                |
|----------------|-----------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `Filter`       | Optional  | Wildcard match against alarm class `Name`. `*`, `?`.                                                                                                                                                                                                                                                                  |
| `Attributes`   | Optional  | Extra attributes. Response always returns `Name` and `StateMachine`. `"*"` returns: `Name, StateMachine, ID, Priority, NormalStateTextColor, NormalStateBackColor, RaisedStateTextColor, RaisedStateBackColor, RaisedStateFlashing, AcknowledgedStateTextColor, AcknowledgedStateBackColor, AcknowledgedStateFlashing, ClearedStateTextColor, ClearedStateBackColor, ClearedStateFlashing, AcknowledgedClearedStateTextColor, AcknowledgedClearedStateBackColor, AcknowledgedClearedStateFlashing`. |
| `SystemNames`  | Optional  | `"*"` or list. Default = local system.                                                                                                                                                                                                                                                                                |
| `ClientCookie` | Required  | Response cookie name.                                                                                                                                                                                                                                                                                                 |

Success / error (manual mixes basic-syntax-style examples into this section):

```
NotifyBrowseAlarmClasses HMI_RT_1::Alarm HMI_RT_1::SystemNotification HMI_RT_1::SystemInformation
                         HMI_RT_1::SystemAlarm HMI_RT_1::Notification HMI_RT_1::OperatorInputInformation
ErrorBrowseAlarmClasses Invalid arguments passed to browsing function.
```

> In actual use over the expert syntax, expect a JSON envelope of shape `{"Message":"NotifyBrowseAlarmClasses","Params":{"AlarmClasses":[...]}, "ClientCookie":"..."}` and `{"Message":"ErrorBrowseAlarmClasses","ErrorCode":"...","ErrorDescription":"...","ClientCookie":"..."}`.

---

## 6. Alarm Filter Syntax

Used in `SubscribeAlarm` / `ReadAlarm` `Filter` and (for some commands) `BrowseConfiguredAlarms`. The syntax is **SQL `WHERE`-clause-like**, but the keyword `WHERE` itself is **omitted**.

### Operators

| Operator                | Description                                | Example                              |
|-------------------------|--------------------------------------------|--------------------------------------|
| `=`                     | equal to                                   | `Name = 'Recipe246'`                 |
| `<>`                    | not equal                                  | `Value <> 0.0`                       |
| `>`                     | greater than                               | `Value > 25.0`                       |
| `<`                     | less than                                  | `Value < 75.0`                       |
| `>=`                    | greater than or equal to                   | `Value >= 25.0`                      |
| `<=`                    | less than or equal to                      | `Value <= 75.0`                      |
| `OR`, `\|\|`             | logical OR                                 | `State = 1 OR State = 3`             |
| `AND`, `&&`             | logical AND                                | `Value >= 25.0 AND Value <= 75.0`    |
| `BETWEEN`               | within a range                             | `Value BETWEEN 25.0 AND 75.0`        |
| `NOT BETWEEN`           | outside a range                            | `Value NOT BETWEEN 25.0 AND 75.0`    |
| `LIKE <string>`         | matches a string pattern                   | `Name LIKE 'Motor*'`                 |
| `NOT LIKE <string>`     | does not match a string pattern            | `Name NOT LIKE 'Valve*'`             |
| `IN (v1, v2, …)`        | matches one of the listed values           | `State IN (1, 4, 7)`                 |
| `NOT IN (v1, v2, …)`    | does not match any of the listed values    | `State NOT IN (0, 2, 3, 5, 6)`       |
| `( … )`                 | grouping                                   | `Value <= 75.0 AND (State = 1 OR State = 3)` |

### Operator precedence (high → low)

1. Relational operators `=, <>, >, <, >=, <=`, `LIKE`, `IN`, `BETWEEN`
2. `NOT`
3. `AND`, `&&`
4. `OR`, `||`

### Wildcards (within `LIKE` / `=` string literals)

| Wildcard | Meaning                  | Example                                           |
|----------|--------------------------|---------------------------------------------------|
| `*`      | 0 or more characters     | `Name LIKE 'Motor*'`, `Reference = '<1.*.15>1'`   |
| `?`      | exactly 1 character      | `Name = 'Recipe?'`                                |

---

## 7. Reference — Tag and Alarm Properties (used by `ReadTag` / `SubscribeTag` / `ReadAlarm` / `SubscribeAlarm`)

All values are transferred as **strings** over the pipe.

### 7.1 Tag properties

| Property           | Description                                                                |
|--------------------|----------------------------------------------------------------------------|
| `Name`             | Name of the tag.                                                           |
| `Value`            | Tag value at the moment of the read.                                       |
| `Quality`          | Read quality. One of `Good`, `Bad`, `Uncertain`.                           |
| `QualityCode`      | Numeric quality code of the read.                                          |
| `TimeStamp`        | Time stamp of the last successful read.                                    |
| `Error`            | Error code of the last read/write of the tag.                              |
| `ErrorDescription` | Description of the error code of the last read/write of the tag.           |

### 7.2 Alarm properties

| Property                                 | Description                                                                                              |
|------------------------------------------|----------------------------------------------------------------------------------------------------------|
| `InstanceID`                             | InstanceID for an alarm with multiple instances.                                                         |
| `SourceID`                               | Source at which the alarm was triggered.                                                                 |
| `Name`                                   | Name of the alarm.                                                                                       |
| `AlarmClassName`                         | Name of the alarm class.                                                                                 |
| `AlarmClassSymbol`                       | Symbol of the alarm class.                                                                               |
| `AlarmParameterValues`                   | Parameter values of the alarm.                                                                           |
| `AlarmText1` … `AlarmText9`              | Additional alarm texts 1–9.                                                                              |
| `ChangeReason`                           | Trigger event of the modification of the alarm state.                                                    |
| `Connection`                             | Connection via which the alarm was triggered.                                                            |
| `State`                                  | Current alarm state. See state values below.                                                             |
| `StateText`                              | Current alarm state as text (e.g. `active`, `inactive`).                                                 |
| `StateMachine`                           | State machine identifier of the alarm class.                                                             |
| `EventText`                              | Text describing the alarm event.                                                                         |
| `InfoText`                               | Operator-instruction text for the alarm.                                                                 |
| `TextColor`                              | Numeric text color of the alarm state.                                                                   |
| `BackColor`                              | Numeric background color of the alarm state.                                                             |
| `Flashing`                               | `TRUE` or `FALSE` — whether the alarm flashes.                                                            |
| `ModificationTime`                       | Time of last modification to the alarm state.                                                            |
| `RaiseTime`                              | Trigger time of the alarm.                                                                               |
| `AcknowledgementTime` / `AcknowledgmentTime` | Time of alarm acknowledgment.                                                                       |
| `ClearTime`                              | Time the alarm was cleared.                                                                              |
| `ResetTime`                              | Time the alarm was reset.                                                                                |
| `SuppressionState`                       | Status of alarm visibility.                                                                              |
| `SystemSeverity`                         | Severity of the system error.                                                                            |
| `Priority`                               | Relevance for display and sorting.                                                                       |
| `Origin`                                 | Origin (display/sorting).                                                                                |
| `Area`                                   | Area (display/sorting).                                                                                  |
| `Value`                                  | Current process value of the alarm.                                                                      |
| `ValueQuality`                           | Quality of the process value.                                                                            |
| `ValueLimit`                             | Limit of the process value.                                                                              |
| `UserName`                               | User name on operator-controlled alarms.                                                                 |
| `HostName`                               | Host that triggered the alarm.                                                                           |
| `ID`                                     | ID of the alarm (also displayed in UIs).                                                                 |
| `AlarmGroupID`                           | ID of the alarm group the alarm belongs to.                                                              |
| `SourceType`                             | `HmiAlarmSourceType` enum: `Undefined(0)`, `Tag(1)`, `Controller(2)`, `System(3)`, `Alarm(4)`.           |
| `DeadBand`                               | Range of the triggering tag in which no alarms are generated.                                            |
| `LoopInAlarm`                            | Function that navigates from the alarm control to its origin.                                            |
| `NotificationReason`                     | Reason for the notification. See values below.                                                           |
| `Duration`                               | Time interval (ns) between alarm trigger and previous status change.                                     |

### 7.3 `State` values

| Value | Meaning                       |
|-------|-------------------------------|
| `0`   | Normal                        |
| `1`   | Raised                        |
| `2`   | RaisedCleared                 |
| `5`   | RaisedAcknowledged            |
| `6`   | RaisedAcknowledgedCleared     |
| `7`   | RaisedClearedAcknowledged     |
| `8`   | Removed                       |

### 7.4 `NotificationReason` values

| Value | Meaning  | Description                                                                                                                                        |
|-------|----------|----------------------------------------------------------------------------------------------------------------------------------------------------|
| `0`   | Unknown  | (Manual prints `"O"`; treat as `0`.)                                                                                                                |
| `1`   | Add      | The alarm was added to the filtered result list (it now meets the filter).                                                                          |
| `2`   | Modify   | A property of the alarm changed but the alarm is still part of the filtered result list.                                                            |
| `3`   | Remove   | The alarm was in the result list but no longer meets the filter criteria. Subsequent property changes generate **no** notifications until it matches again (then `Add`). |

#### Worked example

A subscription with filter `State = 1`. An alarm is then triggered and evolves:

| Notification | Description                                                                                                          |
|--------------|----------------------------------------------------------------------------------------------------------------------|
| `Add`        | `State == 1` — the alarm is active.                                                                                   |
| `Modify`     | `State` unchanged, but a non-filter property (e.g. `Priority`) changed.                                              |
| `Remove`     | `State` changed (e.g. alarm becomes inactive) — no longer matches the filter.                                         |

#### Client patterns

* **State-based monitoring** (e.g. live alarm list): act on all reasons; remove the alarm from your view when `Remove` arrives.
* **Event-based monitoring** (e.g. send an email per new alarm): only act on `Add`.

---

## 8. Error handling notes

* Every command has its own `Error<Command>` (or `Notify<Command>` with per-object `ErrorCode`/`ErrorDescription` for bulk operations).
* The manual lists only **a selection** of error texts; actual messages may differ project-to-project. Examples seen in the manual:
  * `Tag does not exist`
  * `Encoding error`
  * `Value contains newline`
  * `Subscription already exists`
  * `Subscription does not exist`
  * `Subscription could not be created`
  * `Subscription could not be closed`
  * `Failed to Read`
  * `Failed to Write`
  * `unknown character set`
  * `Invalid arguments passed to browsing function.`
  * `A parameter is not valid or out of range.`
  * `Invalid system name.`
  * `Your browse request has been expired`
  * `Alarm Subscription failed because of invalid filter`
* Numeric `ErrorCode` values are large negative `int32` values, e.g. `-2147483620`, `-2147483621`, `-2165322729`, `-2165322733`, `-2165323798`, `-2165322773`.

---

## 9. Implementation checklist for clients

1. **Open the named pipe**
   * Windows: `CreateFile(\\.\pipe\HmiRuntime, GENERIC_READ|GENERIC_WRITE, …)` then `ReadFile`/`WriteFile` (or use language-native named pipe libs).
   * Linux: open `/tmp/HmiRuntime` as a FIFO.
   * Run as a user in `SIMATIC HMI` (Windows) or `industrial` (Linux).
2. **Choose syntax**
   * If you control the protocol from a shell, basic syntax is enough.
   * Otherwise, prefer expert syntax (JSON), set a `ClientCookie` per logical operation, and process responses by matching `ClientCookie`.
3. **Frame each request as a single line** terminated with `\n`.
4. **Read responses line-by-line** off the same pipe.
5. **For browse commands**:
   * Optionally call `WriteConfig` to set `DefaultPageSize` / `BrowseTimeOut`.
   * Send the initial request; loop `Next` requests until an empty response.
   * Use the same `ClientCookie` for all `Next` requests of a single logical browse.
6. **For subscriptions (`SubscribeTag` / `SubscribeAlarm`)**:
   * Keep the `ClientCookie` — it is required to call `UnsubscribeTag` / `UnsubscribeAlarm`.
   * Subscriptions remain across many notifications until you unsubscribe.
7. **Encoding**: default UTF-8; if interoperating with a legacy CMD on Windows you may need `SetCharSet cp850` (DE) / `cp437` (US). Expert syntax is **always UTF-8**.
8. **Avoid newlines in tag values** — they cannot be transported in basic syntax (an error is signaled or the value is truncated on write).
