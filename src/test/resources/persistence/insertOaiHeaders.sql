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


INSERT INTO "OAIHeader" ("recordIdentifier", "datestamp" , "statusIsDeleted") VALUES ('oai:example.org:qucosa:47', '2016-07-10 10:10:40+02', false);
INSERT INTO "OAIHeader" ("recordIdentifier", "datestamp" , "setSpec", "statusIsDeleted") VALUES ('oai:example.org:qucosa:199', '2015-07-10 13:13:13+02', ARRAY['test', 'test," with separator and quotes'], true);