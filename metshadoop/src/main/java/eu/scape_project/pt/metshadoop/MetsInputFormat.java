/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package eu.scape_project.pt.metshadoop;

import java.io.IOException;
import java.util.logging.Level;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetsInputFormat extends FileInputFormat<Text, DTO> {

	private static final Logger LOG = LoggerFactory.getLogger(MetsInputFormat.class);
    public static String TAG = "MetsInputFormat.XmlTag";

    @Override
    public RecordReader<Text, DTO> createRecordReader(InputSplit split,
            TaskAttemptContext context) {

        try {
            LOG.debug("split.length = " + split.getLength());
            LOG.debug("split.string = " + split.toString());
        } catch (IOException ex) {
            LOG.error(ex.getMessage());
        } catch (InterruptedException ex) {
            LOG.error(ex.getMessage());
        }
        String tag = context.getConfiguration().get(MetsInputFormat.TAG);
        return new MetsRecordReader(tag);
    }
}