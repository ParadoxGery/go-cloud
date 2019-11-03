// Copyright 2018-2019 The Go Cloud Development Kit Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

module gocloud.dev/pubsub/redispubsub

go 1.13

require (
	github.com/alicebob/miniredis/v2 v2.10.1
	github.com/go-redis/redis v6.15.6+incompatible
	github.com/go-redis/redis/v7 v7.0.0-beta.4
	github.com/gomodule/redigo v2.0.0+incompatible // indirect
	github.com/yuin/gopher-lua v0.0.0-20190514113301-1cd887cd7036 // indirect
	gocloud.dev v0.17.0
)

replace gocloud.dev => ../../
