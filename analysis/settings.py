# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# mysql database Config
DATABASES = {
    'alatting': {
        'engine': 'mysql',
        'db': 'alatting',
        'user': 'root',
        'password': '123456',
        'host': '192.168.1.36',
        'port': 3306
    },
}

address_list = ["湖南省","湖北省","广东省","广西省","河南省","河北省",
                "山东省","山西省","江苏省","浙江省","江西省","黑龙江省",
                "新疆省", "云南省","贵州省","福建省","吉林省","安徽省",
                "四川省","西藏","宁夏","辽宁省","青海省","甘肃省","陕西省",
                "内蒙古","台湾省","北京市","上海市","天津市"]