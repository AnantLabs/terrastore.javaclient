/**
 * Copyright 2009 - 2011 Sergio Bossa (sergio.bossa@gmail.com)
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
 * General exception super type for checked exceptions thrown by the
 * {@link TerrastoreClient}.
 * 
 * Client code will benefit from catching the more granular sub-types of this
 * exception in order to handle specific failures.
 * 
 * @author Sven Johansson
 * @date 24 apr 2010
 *  
 */
public class TerrastoreClientException extends RuntimeException {

    private static final long serialVersionUID = 8837607736588767483L;

    TerrastoreClientException() {
    }

    public TerrastoreClientException(String message) {
        super(message);
    }

    public TerrastoreClientException(String message, Throwable cause) {
        super(message, cause);
    }

}
