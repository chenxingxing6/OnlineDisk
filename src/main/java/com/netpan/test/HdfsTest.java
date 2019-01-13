package com.netpan.test;

import com.netpan.dao.conn.HdfsConn;
import com.netpan.entity.File;
import com.netpan.entity.User;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.springframework.stereotype.Repository;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

public class HdfsTest {
	private final String basePath = "/cxx/";
	
	/**
	 * 获得在hdfs中的目录
	 * @param file
	 * @param user
	 * @return
	 */
	private String formatPathMethod(User user, File file) {
		return basePath + user.getName() + file.getPath();
	}
	
	/**
	 * 上传文件
	 */
	public void put(InputStream inputStream, File file, User user) {
		try {
			String formatPath = formatPathMethod(user, file);
			OutputStream outputStream = HdfsConn.getFileSystem().create(new Path(formatPath), new Progressable() {
				public void progress() {
					//System.out.println("upload OK");
				}
			});
			IOUtils.copyBytes(inputStream, outputStream, 2048, true);
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 创建文件夹
	 * @param file
	 * @param user
	 */
	public void mkDir(File file, User user) {
		try {
			String formatPath = formatPathMethod(user, file);
			if (!HdfsConn.getFileSystem().exists(new Path(formatPath))) {
				HdfsConn.getFileSystem().mkdirs(new Path(formatPath));
			}
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 删除文件或目录
	 * @param file
	 * @param user
	 */
	public void delete(File file, User user) {
		try {
			String formatPath = formatPathMethod(user, file);
			if (HdfsConn.getFileSystem().exists(new Path(formatPath))) {
				HdfsConn.getFileSystem().delete(new Path(formatPath), true);
			}
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 重命名文件，未使用
	 * @param file
	 * @param user
	 * @param newname
	 */
	public void rename(File file, User user, String newname) {
		try {
			String formatPath = formatPathMethod(user, file);
			file.setName(newname);
			String newformatPath = formatPathMethod(user, file);
			if (HdfsConn.getFileSystem().exists(new Path(formatPath))) {
				HdfsConn.getFileSystem().rename(new Path(formatPath), new Path(newformatPath));
			}
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 下载文件
	 * @param user
	 * @param file
	 * @param local
	 */
	public boolean download(User user, File file, String local) {
		try {
			String formatPath = formatPathMethod(user, file);
			if (HdfsConn.getFileSystem().exists(new Path(formatPath))) {
				FSDataInputStream inputStream = HdfsConn.getFileSystem().open(new Path(formatPath));
				OutputStream outputStream = new FileOutputStream(local);
				IOUtils.copyBytes(inputStream, outputStream, 4096, true);
				System.out.println(local);
				return true;
			}
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return false;
	}
	
	/**
	 * 复制或者移动文件或者目录
	 */
	public void copyOrMove(User user, File sourceFile, File destFile, boolean flag) {
		try {
			String sourceFormatPath = formatPathMethod(user, sourceFile);
			String destFormatPath = formatPathMethod(user, destFile);
			FileUtil.copy(HdfsConn.getFileSystem(), new Path(sourceFormatPath), HdfsConn.getFileSystem(), new Path(destFormatPath), flag, true, HdfsConn.getConfiguration());
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

/*	public static void main(String[] args) {
		HdfsTest hdfsTest = new HdfsTest();
		User user = new User();
		user.setName("cxx");
		hdfsTest.mkDir(new File(), user);
	}*/

	public static void main(String[] args) throws Exception {
		String uri="hdfs://192.168.153.134:9000/test/test.txt";
		System.setProperty("hadoop.home.dir", "E:\\hadoop-2.8.3");
		Configuration configuration=new Configuration();
		FileSystem fileSystem= FileSystem.get(URI.create(uri), configuration);
		FSDataInputStream in=null;
		in=fileSystem.open(new Path(uri));
//		FileStatus fileStatus=fileSystem.getFileStatus(new Path(uri));
//		byte[] buffer=new byte[1024];
//		in.read(4096, buffer, 0, 1024);
		IOUtils.copyBytes(in, System.out, 4096, false);
		IOUtils.closeStream(in);
	}
	
	
}
