--
--  Copyright 2016 SLUB Dresden
-- 
--  Licensed under the Apache License, Version 2.0 (the "License");
--  you may not use this file except in compliance with the License.
--  You may obtain a copy of the License at
-- 
--  http://www.apache.org/licenses/LICENSE-2.0
-- 
--  Unless required by applicable law or agreed to in writing, software
--  distributed under the License is distributed on an "AS IS" BASIS,
--  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
--  See the License for the specific language governing permissions and
--  limitations under the License.
-- 


INSERT INTO "OAIRunResult" VALUES (1, '2016-07-19 11:11:40.74+02', '2011-01-03 12:00:23-03', '', NULL, NULL);
INSERT INTO "OAIRunResult" VALUES (2, '2016-07-20 13:18:40.038+02', '2011-01-03 12:00:23-03', '', NULL, '2016-07-19 11:11:40.74+02');
INSERT INTO "OAIRunResult" VALUES (3, '2016-07-20 13:22:57.137+02', '2011-01-03 12:00:23-03', '140225245500000', '2014-06-09 20:34:15+04', '2016-07-20 13:18:40.038+02');

SELECT pg_catalog.setval('"oairunresult_ID_seq"', 3, true);