package com.csl.quartz;

import io.netty.channel.Channel;

import java.io.File;
import java.util.List;
import java.util.Map;

import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.log4j.Logger;
import org.quartz.DateBuilder;
import org.quartz.Job;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.SimpleTrigger;
import org.quartz.TriggerBuilder;
import org.quartz.TriggerKey;
import org.quartz.impl.StdSchedulerFactory;

import ch.ethz.ssh2.Connection;
import ch.ethz.ssh2.Session;

import com.csl.base.SqlSessionFactorySingleton;
import com.csl.dao.CslServerDAO;
import com.csl.hpp.HPPDao;
import com.csl.hpp.HPPRoomUtil;
import com.csl.hpp.MessageHelper;
import com.csl.hpp.MessageWraper;
import com.csl.hpp.WebSocketUtil;
import com.csl.model.CslServer;

public class CheckHPPRoomJob implements Job {
	private static Logger log = Logger.getLogger(CheckHPPRoomJob.class);

	public void execute(JobExecutionContext arg0) throws JobExecutionException {

		log.info("CheckHPPRoomJob execute");
		SqlSessionFactory sf = SqlSessionFactorySingleton.getSessionFactory();
		SqlSession session = sf.openSession();
		try {
			HPPDao dao = session.getMapper(HPPDao.class);
			List<Map<String, Object>> rooms = dao.checkLoseRoom(System.currentTimeMillis() - 90000);
			int size = rooms.size();
			if (size == 0) {

				log.debug("所有hpp房间心跳正常(90000检测)=================================");
				return;
			}
			log.debug("有hpp房间心跳失败");

			StringBuilder sb = new StringBuilder();

			for (int i = 0; i < size; i++) {

				Map<String, Object> map = rooms.get(i);
				log.debug(map);
				if (i > 0) {
					sb.append(",");

				}
				sb.append(HPPRoomUtil.getRoomId(map));
			}
			CslServerDAO serDao = new CslServerDAO(session);
			CslServer server = serDao.findServerToCreateRoom();
			//server.getOutsideIp()
			log.debug(server.getOutsideIp() + ":8089 = >尝试是否存在hpp服务器");
			Channel channel = WebSocketUtil.getChannel("192.168.0.51", 8089);
			if (channel == null) {
				log.debug("hpp服务器不存在 调用linux 命令=============");
				String username = server.getUserName();
				String password = server.getPassWord();
				Session sess = null;
				Connection conn = null;
				try {
					conn = new Connection(server.getOutsideIp());
					conn.connect();			
					File pemFile = new File("/home/tpuser/.ssh/id_rsa");
					boolean isAuthenticated = conn.authenticateWithPublicKey(username, pemFile, password);
					if (!conn.authenticateWithPassword(username, password)) {
						log.warn("远程授权失败===================");
						return;
					}
					sess = conn.openSession();

					String command = null;
					
					StringBuilder str=new StringBuilder();
					
					str.append("cd /mnt/hpp/bin;");
					str.append("sh startup.sh ").append(sb);
					command=str.toString();
					log.info("cmd:"+command);
					sess.execCommand(command);
					log.info("cmd over");
					Thread.sleep(1000);

				} catch (Exception e) {
					log.error(e.getMessage(), e);

				} finally {

					if (sess != null) {
						sess.close();
					}

					if (conn != null) {
						conn.close();
					}
				}
			} else {
				log.debug("存在hpp服务器 发送启动房间指令");
				
				log.debug("channel.isActive()="+channel.isActive()+",channel.isOpen()="+channel.isOpen()+",channel.isWritable()="+channel.isWritable()+",channel.isOpen()="+channel.isOpen());
				MessageWraper mw = MessageHelper.prepReqMessage("hpp-room").append("method", "clusterStart")
						.append("rooms", sb.toString());

				channel.writeAndFlush(mw.ToBinaryWebSocketFrame());

			}

		} catch (Exception e) {
			log.debug("", e);
			session.rollback();
		} finally {
			session.close();
		}

	
	}
	public void execute1(JobExecutionContext arg0) throws JobExecutionException {
		log.info("CheckHPPRoomJob execute");
		SqlSessionFactory sf = SqlSessionFactorySingleton.getSessionFactory();
		SqlSession session = sf.openSession();
		try {
			HPPDao dao = session.getMapper(HPPDao.class);
			List<Map<String, Object>> rooms = dao.checkLoseRoom(System.currentTimeMillis() - 90000);
			int size = rooms.size();
			if (size == 0) {

				log.debug("所有hpp房间心跳正常(90000检测)=================================");
				return;
			}
			log.debug("有hpp房间心跳失败");

			StringBuilder sb = new StringBuilder();

			for (int i = 0; i < size; i++) {

				Map<String, Object> map = rooms.get(i);
				log.debug(map);
				if (i > 0) {
					sb.append(",");

				}
				sb.append(HPPRoomUtil.getRoomId(map));
			}
			CslServerDAO serDao = new CslServerDAO(session);
			CslServer server = serDao.findServerToCreateRoom();
			log.debug(server.getOutsideIp() + ":1234 = >尝试是否存在hpp服务器");
			Channel channel = WebSocketUtil.getChannel("192.168.0.51", 8089);
			if (channel == null) {
				log.debug("hpp服务器不存在 调用linux 命令=============");
				String username = server.getUserName();
				String password = server.getPassWord();
				Session sess = null;
				Connection conn = null;
				try {
					conn = new Connection(server.getOutsideIp());
					conn.connect();					
					if (!conn.authenticateWithPassword(username, password)) {
						log.warn("远程授权失败===================");
						return;
					}
					sess = conn.openSession();

					String command = null;
					
					StringBuilder str=new StringBuilder();
					
					str.append("cd /mnt/hpp/bin;");
					str.append("sh startup.sh ").append(sb);
					command=str.toString();
					log.info("cmd:"+command);
					sess.execCommand(command);
					log.info("cmd over");
					Thread.sleep(1000);

				} catch (Exception e) {
					log.error(e.getMessage(), e);

				} finally {

					if (sess != null) {
						sess.close();
					}

					if (conn != null) {
						conn.close();
					}
				}
			} else {
				log.debug("存在hpp服务器 发送启动房间指令");
				
				log.debug("channel.isActive()="+channel.isActive()+",channel.isOpen()="+channel.isOpen()+",channel.isWritable()="+channel.isWritable()+",channel.isOpen()="+channel.isOpen());
				MessageWraper mw = MessageHelper.prepReqMessage("hpp-room").append("method", "clusterStart")
						.append("rooms", sb.toString());

				channel.writeAndFlush(mw.ToBinaryWebSocketFrame());

			}

		} catch (Exception e) {
			log.debug("", e);
			session.rollback();
		} finally {
			session.close();
		}

	}

	public static void start() {
		Scheduler scheduler;
		try {
			scheduler = StdSchedulerFactory.getDefaultScheduler();
			JobDetail jd = JobBuilder.newJob(CheckHPPRoomJob.class).build();

			SimpleTrigger trigger = TriggerBuilder.newTrigger()
					.withIdentity(TriggerKey.triggerKey("CheckHPPRoomJob", "CheckHPPRoomJob"))
					.withSchedule(SimpleScheduleBuilder.simpleSchedule().withIntervalInSeconds(60).repeatForever())
					.startAt(DateBuilder.futureDate(60, DateBuilder.IntervalUnit.SECOND)).build();

			if (!scheduler.checkExists(trigger.getKey())) {
				scheduler.scheduleJob(jd, trigger);
			}

		} catch (SchedulerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public static void main(String[] args) {
		Session sess = null;
		Connection conn = null;
		try {
			conn = new Connection("172.31.27.227");

			conn.connect();

			boolean isAuthenticated = conn.authenticateWithPassword("root", "89adf064");

			if (!isAuthenticated ) {
				log.warn("远程授权失败===================");
				return;
			}
			sess = conn.openSession();

			String command = null;
			
			StringBuilder str=new StringBuilder();
	
		
			str.append("cd /mnt/hpp/bin;");
			str.append("sh startup.sh 125");
			//str.append("cd /home/tpuser/hpp/bin;sh startup.sh 124,125,128,130");
			command=str.toString();
			log.info("cmd:"+command);
			sess.execCommand(command);

		} catch (Exception e) {
			log.error(e.getMessage(), e);

		} finally {

			if (sess != null) {
				sess.close();
			}

			if (conn != null) {
				conn.close();
			}
		}
	}
}