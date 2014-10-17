package org.apache.storm.hdfs.bolt;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.ql.io.orc.CompressionKind;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.Writer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
import org.apache.storm.hdfs.common.rotation.RotationAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;

public class ORCFileBolt extends AbstractHdfsBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static final Logger LOG = LoggerFactory
			.getLogger(ORCFileBolt.class);
	private Writer writer = null;
	private Class recordClass = null; // Represents Rows to be stored in ORC
	private long offset = 0;
	private OrcFile.WriterOptions opts;

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

	public ORCFileBolt withRecordClass(Class recordClass) {
		this.recordClass = recordClass;
		return this;
	}

	public ORCFileBolt withFileNameFormat(FileNameFormat fileNameFormat) {
		this.fileNameFormat = fileNameFormat;
		return this;
	}

	public ORCFileBolt withFsUrl(String fsUrl) {
		this.fsUrl = fsUrl;
		return this;
	}

	public ORCFileBolt withConfigKey(String configKey) {
		this.configKey = configKey;
		return this;
	}

	public ORCFileBolt withSyncPolicy(SyncPolicy syncPolicy) {
		this.syncPolicy = syncPolicy;
		return this;
	}

	public ORCFileBolt withRotationPolicy(FileRotationPolicy rotationPolicy) {
		this.rotationPolicy = rotationPolicy;
		return this;
	}

	public ORCFileBolt addRotationAction(RotationAction action) {
		this.rotationActions.add(action);
		return this;
	}

	void closeOutputFile() throws IOException {
		writer.close();
		

	}

	Path createOutputFile() throws IOException {
		Path p = new Path(this.fsUrl + this.fileNameFormat.getPath(),
				this.fileNameFormat.getName(this.rotation,
						System.currentTimeMillis()));

		// HardCoded to Static values for now
		writer = OrcFile.createWriter(p, opts);

		return p;
	}

	void doPrepare(Map conf, TopologyContext topologyContext,
			OutputCollector collector) throws IOException {
		LOG.info("Preparing ORCFile Bolt...");
		this.fs = FileSystem.get(URI.create(this.fsUrl), hdfsConfig);

		if (recordClass == null) {
			LOG.error("RecordClass Must be Specified");
			System.exit(-1);
		}

		Configuration config = new Configuration();
		ObjectInspector inspector = ObjectInspectorFactory
				.getReflectionObjectInspector(recordClass,
						ObjectInspectorFactory.ObjectInspectorOptions.JAVA);

		opts = OrcFile.writerOptions(config);
		opts.stripeSize(1024*1024*3);
		opts.blockPadding(false);
		opts.compress(CompressionKind.SNAPPY);
		opts.inspector(inspector);
		opts.rowIndexStride(0);
		opts.bufferSize(1024*1024*3);

	}
	
	public void cleanup() 
	{
		try {
			writer.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void execute(Tuple tuple) {
		// TODO Auto-generated method stub
		synchronized (this.writeLock) {

			Object row = tuple.getValue(0); // Row to be Written
			// out.write(bytes);
			try {
				writer.addRow(row);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			this.offset++; //this should ideally be #bytes. We are treating it as rows

			if (this.syncPolicy.mark(tuple, this.offset)) {
				this.syncPolicy.reset();
			}
		}
		this.collector.ack(tuple);

		if (this.rotationPolicy.mark(tuple, this.offset)) {
			try {
				rotateOutputFile();// synchronized
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			this.offset = 0;
			this.rotationPolicy.reset();
		}
	}
}
