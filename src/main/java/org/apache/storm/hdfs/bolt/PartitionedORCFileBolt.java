package org.apache.storm.hdfs.bolt;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

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
import org.apache.storm.hdfs.common.rotation.MoveToPartitionAction;
import org.apache.storm.hdfs.common.rotation.RotationAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;

public class PartitionedORCFileBolt extends AbstractHdfsBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static final Logger LOG = LoggerFactory
			.getLogger(PartitionedORCFileBolt.class);
	private Writer writer = null;
	private Map<String, Writer> writerMap = null;
	private Map<String, Path> filePathMap = null;
	private Class recordClass = null; // Represents Rows to be
										// stored in ORC
	private long offset = 0;
	private OrcFile.WriterOptions opts;
	private String workingPath = null;
	private String finalPath = null;
	private long synctime=System.currentTimeMillis();

	public PartitionedORCFileBolt withWorkingPath(String path) {
		this.workingPath = path;
		return this;
	}

	public PartitionedORCFileBolt withFinalPath(String path) {
		this.finalPath = path;
		return this;
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

	public PartitionedORCFileBolt withRecordClass(Class recordClass) {
		this.recordClass = recordClass;
		return this;
	}

	public PartitionedORCFileBolt withFileNameFormat(
			FileNameFormat fileNameFormat) {
		this.fileNameFormat = fileNameFormat;
		return this;
	}

	public PartitionedORCFileBolt withFsUrl(String fsUrl) {
		this.fsUrl = fsUrl;
		return this;
	}

	public PartitionedORCFileBolt withConfigKey(String configKey) {
		this.configKey = configKey;
		return this;
	}

	public PartitionedORCFileBolt withSyncPolicy(SyncPolicy syncPolicy) {
		this.syncPolicy = syncPolicy;
		return this;
	}

	public PartitionedORCFileBolt withRotationPolicy(
			FileRotationPolicy rotationPolicy) {
		this.rotationPolicy = rotationPolicy;
		return this;
	}

	public PartitionedORCFileBolt addRotationAction(RotationAction action) {
		this.rotationActions.add(action);
		return this;
	}

	void closeOutputFile() throws IOException {

		for (Writer w : writerMap.values())
			w.close();

		writerMap.clear();

	}

	private Writer getWriter(String partitionName) {

		Writer retVal = writerMap.get(partitionName);

		if (null == retVal) {

			retVal = allocateWriter(partitionName);
			writerMap.put(partitionName, retVal);

		}

		return retVal;
	}

	private Writer allocateWriter(String partitionString) {
		// TODO Auto-generated method stub

		Path p = new Path(this.fsUrl + workingPath + partitionString,
				this.fileNameFormat.getName(this.rotation,
						System.currentTimeMillis()));

		// HardCoded to Static values for now
		Writer writer = null;
		try {
			writer = OrcFile.createWriter(p, opts);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		LOG.info("Allocated new Writer for:" + partitionString);
		filePathMap.put(partitionString, p);
		return writer;
	}

	@Override
	Path createOutputFile() throws IOException {
		Path p = new Path(this.fsUrl + this.fileNameFormat.getPath(),
				this.fileNameFormat.getName(this.rotation,
						System.currentTimeMillis()));

		// HardCoded to Static values for now
		// writer = OrcFile.createWriter(p, opts);

		return p;
	}

	void doPrepare(Map conf, TopologyContext topologyContext,
			OutputCollector collector) throws IOException {
		LOG.info("Preparing PartionedORCFile Bolt...");
		this.fs = FileSystem.get(URI.create(this.fsUrl), hdfsConfig);

		if (recordClass == null) {
			LOG.error("RecordClass Must be Specified");
			throw new RuntimeException("RecordClass Must be Specified");
		}

		Configuration config = new Configuration();
		ObjectInspector inspector = ObjectInspectorFactory
				.getReflectionObjectInspector(recordClass,
						ObjectInspectorFactory.ObjectInspectorOptions.JAVA);

		opts = OrcFile.writerOptions(config);
		opts.stripeSize(1024 * 1024 * 64);
		opts.blockPadding(true);
		opts.compress(CompressionKind.SNAPPY);
		opts.inspector(inspector);
		opts.rowIndexStride(50000);
		opts.bufferSize(1024 * 1024 * 3);
		writerMap = new HashMap<String, Writer>();
		filePathMap = new HashMap<String, Path>();

		MoveToPartitionAction defaultAction = new MoveToPartitionAction();
		defaultAction.setDestinations(workingPath, finalPath);
		defaultAction.setPathMap(filePathMap);
		this.addRotationAction(defaultAction);

	}

	public void execute(Tuple tuple) {

		String partitionName = tuple.getString(0); // Partition Info
		Object row = tuple.getValue(1); // Row to be Written

		// synchronized (this.writeLock)
		{

			try {
				getWriter(partitionName).addRow(row);
			} catch (IOException e) {
				e.printStackTrace();
			}
			this.offset++; // this should ideally be #bytes. We are treating it
							// as rows

			if (this.syncPolicy.mark(tuple, this.offset)) {
				LOG.info("Sync took {} milliseconds",System.currentTimeMillis() - synctime);
				synctime = System.currentTimeMillis();
				
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
