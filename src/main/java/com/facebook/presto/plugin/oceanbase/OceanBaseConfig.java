/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.plugin.oceanbase;


import com.facebook.airlift.configuration.Config;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import java.math.RoundingMode;

/**
 * To get the custom properties to connect to the database. User, password and
 * URL is provided by de BaseJdbcClient is not required. If there is another
 * custom configuration it should be put in here.
 * 
 * @author Marcelo Paes Rech
 *
 */
public class OceanBaseConfig {

	private boolean synonymsEnabled;
	private int varcharMaxSize = 4000;
	private int timestampDefaultPrecision = 6;
	private int numberDefaultScale = 10;
	private RoundingMode numberRoundingMode;

	public OceanBaseConfig() {
		this.numberRoundingMode = RoundingMode.HALF_UP;
	}

	@NotNull
	public boolean isSynonymsEnabled() {
		return this.synonymsEnabled;
	}

	@Config("oceanbase.synonyms.enabled")
	public OceanBaseConfig setSynonymsEnabled(boolean enabled) {
		this.synonymsEnabled = enabled;
		return this;
	}

	@Min(0L)
	@Max(38L)
	public int getNumberDefaultScale() {
		return this.numberDefaultScale;
	}

	@Config("oceanbase.number.default-scale")
	public OceanBaseConfig setNumberDefaultScale(int numberDefaultScale) {
		this.numberDefaultScale = numberDefaultScale;
		return this;
	}

	@NotNull
	public RoundingMode getNumberRoundingMode() {
		return this.numberRoundingMode;
	}

	@Config("oceanbase.number.rounding-mode")
	public OceanBaseConfig setNumberRoundingMode(RoundingMode numberRoundingMode) {
		this.numberRoundingMode = numberRoundingMode;
		return this;
	}

	@Min(4000L)
	public int getVarcharMaxSize() {
		return this.varcharMaxSize;
	}

	@Config("oceanbase.varchar.max-size")
	public OceanBaseConfig setVarcharMaxSize(int varcharMaxSize) {
		this.varcharMaxSize = varcharMaxSize;
		return this;
	}

	@Min(0L)
	@Max(9L)
	public int getTimestampDefaultPrecision() {
		return this.timestampDefaultPrecision;
	}

	@Config("oceanbase.timestamp.precision")
	public OceanBaseConfig setTimestampDefaultPrecision(int timestampDefaultPrecision) {
		this.timestampDefaultPrecision = timestampDefaultPrecision;
		return this;
	}

}
