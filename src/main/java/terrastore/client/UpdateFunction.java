/**
 * Copyright 2009 Sergio Bossa (sergio.bossa@gmail.com)
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package terrastore.client;

/**
 * Enum type that provides the available Update functions.
 * 
 * @author Sven Johansson
 * @date 25 apr 2010
 * @since 2.0
 */
public enum UpdateFunction {
    REPLACE("replace");

    private final String functionId;

    UpdateFunction(String functionId) {
        this.functionId = functionId;
    }

    public String functionId() {
        return functionId;
    }

}
