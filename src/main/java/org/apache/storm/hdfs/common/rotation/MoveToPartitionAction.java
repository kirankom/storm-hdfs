package org.apache.storm.hdfs.common.rotation;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

public class MoveToPartitionAction implements RotationAction {
	private static final Logger LOG = LoggerFactory
			.getLogger(MoveToPartitionAction.class);

	private String workingPath, finalPath;
	private Map<String, Path> pathMap;

	public MoveToPartitionAction setDestinations(String workingPath,
			String finalPath) {
		this.finalPath = finalPath;
		this.workingPath = workingPath;
		LOG.info("Settings Source and Target Desitinations to {} to {}",workingPath,finalPath);
		return this;
	}

	public MoveToPartitionAction setPathMap(Map<String, Path> map) {
		this.pathMap = map;
		return this;
	}

	// @Override
	public void execute(FileSystem fileSystem, Path filePath)
			throws IOException {
		LOG.info("Attempting to Move Files in {} to {}",workingPath,finalPath);
			
		for (Path path : pathMap.values()) {
			Path destPath = new Path(path.toUri().toString()
					.replaceFirst(workingPath, finalPath));
			LOG.info("Moving file {} to {}", path, destPath);
			boolean success = fileSystem.rename(path, destPath);
		}
		pathMap.clear();
		return;
	}
}
