/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import fs from 'fs'
import path from 'path'
import process from 'process'
import { fileURLToPath } from 'url'

const __filename = fileURLToPath(import.meta.url)

const __dirname = path.dirname(__filename)

const filePath = path.resolve(__dirname, './src')

const fileFilter = /\.(vue|ts)$/
const excludeFiles = ['auto-imports.d.ts', 'components.d.ts']

const tsLicense = `
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
`

const vueLicense = `
<!--
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
-->
`

function fileDisplay(filePath) {
  fs.readdir(filePath, async function (err, files) {
    if (err) {
      console.log('\x1b[31m get file list error\x1b[0m: ' + err)
      process.exit(1)
    } else {
      for (let index = 0; index < files.length; index++) {
        const filename = files[index]
        const fileDir = path.join(filePath, filename)
        const isDir = await fs.statSync(fileDir).isDirectory()

        if (isDir) {
          await fileDisplay(fileDir)
        } else if (fileFilter.test(filename) && !excludeFiles.includes(filename)) {
          checkLicense(fileDir)
        }
      }
    }
  })
}

function checkLicense(path) {
  const ext = path.split('.').pop()
  const license = ext === 'vue' ? vueLicense : tsLicense
  const content = fs.readFileSync(path, 'utf-8')

  if (!content.startsWith(license.trim())) {
    console.error('\x1b[31m license check error\x1b[0m: ' + path)
    process.exit(1)
  } else {
    console.log('\x1b[32m license check success\x1b[0m: ' + path)
  }
}

fileDisplay(filePath)
