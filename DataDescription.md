Hotels data.

| Column name | Description | Data type |
| --- | --- | --- |
| Id | ID of hotel | string |
| Name | Hotel name | string |
| Country | Hotel country | string |
| City | Hotel city | string |
| Address | Hotel address | string |
| Latitude | Latitude | string |
| Longitude | Longitude | string |

Weather data.

| Column name | Description | Data type | Partition |
| --- | --- | --- | --- |
| lng | Longitude of a weather station | double | no |
| lat | Latitude of a weather station | double | no |
| avg_tmpr_f | Average temperature in Fahrenheit | double | no |
| avg_tmpr_c | Average temperature in Celsius | double | no |
| wthr_date | Date of observation (YYYY-MM-DD) | string | no |
| wthr_year | Year of observation (YYYY) | string | yes |
| wthr_month | Month of observation (MM) | string | yes |
| wthr_day | Day of observation (DD) | string | yes |
