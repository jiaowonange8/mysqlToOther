package com.nange;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;
import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import javax.swing.JTextField;

import com.nange.constant.DatabaseProperties;
import com.nange.constant.DatabaseType;
import com.nange.datasource.DatabaseExeHandler;
import com.nange.datasource.DatabaseFactory;

public class SwingMain {
	
	private static DataSource sourceDataSource;//源数据库-数据源
	private static DataSource targetDataSource;//目标数据库-数据源
	
	
	public static void main(String[] args) {
		// 创建 JFrame 实例
        JFrame frame = new JFrame("数据库迁移");
        // Setting the width and height of frame
        frame.setSize(500, 800);
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        JPanel panel = new JPanel();    
        // 添加面板
        frame.add(panel);
        placeComponents(panel);
        // 设置界面可见
        frame.setVisible(true);
    }

    private static void placeComponents(JPanel panel) {

        panel.setLayout(null);
        
        final JTextArea result = new JTextArea();
        JScrollPane jsp = new JScrollPane(result);
        jsp.setBounds(40,450,400,200);
        jsp.setVerticalScrollBarPolicy( JScrollPane.VERTICAL_SCROLLBAR_ALWAYS);
      	panel.add(jsp);

        JLabel fromDataLabel = new JLabel("源数据库类型:");
        fromDataLabel.setBounds(10,20,120,25);
        panel.add(fromDataLabel);
        final JComboBox<String> fromDataBox=new JComboBox<String>();
        fromDataBox.addItem(DatabaseType.MYSQL.name());
        fromDataBox.setBounds(140,20,250,25);
        panel.add(fromDataBox);
        
        JLabel fromIpLabel = new JLabel("源数据库ip:");
        fromIpLabel.setBounds(10,48,120,25);
        panel.add(fromIpLabel);
        final JTextField fromIpField = new JTextField();
        fromIpField.setBounds(140,48,250,25);
        fromIpField.setText("127.0.0.1");
        panel.add(fromIpField);
        
        
        JLabel fromPortLabel = new JLabel("源数据库端口:");
        fromPortLabel.setBounds(10,75,120,25);
        panel.add(fromPortLabel);
        final JTextField fromPortField = new JTextField();
        fromPortField.setBounds(140,75,250,25);
        fromPortField.setText("3306");
        panel.add(fromPortField);

        JLabel fromDatanameLabel = new JLabel("源数据库库名:");
        fromDatanameLabel.setBounds(10,102,120,25);
        panel.add(fromDatanameLabel);
        final JTextField fromDatanameField = new JTextField();
        fromDatanameField.setBounds(140,102,250,25);
        fromDatanameField.setText("mysqlconvert");
        panel.add(fromDatanameField);
        
        JLabel fromUsernameLabel = new JLabel("源数据库库用户名:");
        fromUsernameLabel.setBounds(10,129,120,25);
        panel.add(fromUsernameLabel);
        final JTextField fromUsernameField = new JTextField();
        fromUsernameField.setBounds(140,129,250,25);
        fromUsernameField.setText("qiyuesuo");
        panel.add(fromUsernameField);
        
        JLabel fromPasswordLabel = new JLabel("源数据库库密码:");
        fromPasswordLabel.setBounds(10,156,120,25);
        panel.add(fromPasswordLabel);
        final JTextField fromPasswordField = new JTextField();
        fromPasswordField.setBounds(140,156,250,25);
        fromPasswordField.setText("qiyuesuo");
        panel.add(fromPasswordField);
        
        JButton fromButtonTest = new JButton("test");
        fromButtonTest.setBounds(400, 100, 80, 40);
        panel.add(fromButtonTest);
        
        fromButtonTest.addActionListener(new ActionListener(){
            @Override
            public void actionPerformed(ActionEvent e) {
            	final String fromType = fromDataBox.getSelectedItem().toString();
            	final String fromIp = fromIpField.getText();
            	final String fromPort = fromPortField.getText();
            	final String fromDatabase = fromDatanameField.getText();
            	final String fromUsername = fromUsernameField.getText();
            	final String fromPassword = fromPasswordField.getText();
            	new Thread(new Runnable() {
					@Override
					public void run() {
						try {
							Connection testConnection = testConnection(fromType, fromDatabase, fromPassword, fromPort, fromIp, fromUsername);
							testConnection.close();
							result.append("success connect "+fromType+"\r\n");
						} catch (Exception e1) {
							result.append("ERROR "+e1.getMessage()+"\r\n");
						}
						
					}
				}).start();
            }
        });
        //==================from end==========================
        JLabel fengeLabel = new JLabel("====================================================================");
        fengeLabel.setBounds(10,200,500,25);
        panel.add(fengeLabel);

        
        JLabel toDataLabel = new JLabel("目标数据库类型:");
        toDataLabel.setBounds(10,240,120,25);
        panel.add(toDataLabel);
        final JComboBox<String> toDataBox=new JComboBox<String>();
        toDataBox.addItem(DatabaseType.OPENGAUSS.name());
        toDataBox.addItem(DatabaseType.KINGBASEES.name());
        toDataBox.addItem(DatabaseType.ORACLE.name());
        toDataBox.addItem(DatabaseType.SQLSERVER.name());
        toDataBox.addItem(DatabaseType.DM.name());
        toDataBox.addItem(DatabaseType.OSCAR.name());
        toDataBox.setBounds(140,240,250,25);
        panel.add(toDataBox);
        
        JLabel toIpLabel = new JLabel("目标数据库ip:");
        toIpLabel.setBounds(10,268,120,25);
        panel.add(toIpLabel);
        final JTextField toIpField = new JTextField();
        toIpField.setBounds(140,268,250,25);
        panel.add(toIpField);
        
        
        JLabel toPortLabel = new JLabel("目标数据库端口:");
        toPortLabel.setBounds(10,295,120,25);
        panel.add(toPortLabel);
        final JTextField toPortField = new JTextField();
        toPortField.setBounds(140,295,250,25);
        toPortField.setText("54321");
        panel.add(toPortField);

        JLabel toDatanameLabel = new JLabel("目标数据库库名:");
        toDatanameLabel.setBounds(10,322,120,25);
        panel.add(toDatanameLabel);
        final JTextField toDatanameField = new JTextField();
        toDatanameField.setBounds(140,322,250,25);
        panel.add(toDatanameField);
        
        JLabel toUsernameLabel = new JLabel("目标数据库库用户名:");
        toUsernameLabel.setBounds(10,349,120,25);
        panel.add(toUsernameLabel);
        final JTextField toUsernameField = new JTextField();
        toUsernameField.setBounds(140,349,250,25);
        panel.add(toUsernameField);
        
        JLabel toPasswordLabel = new JLabel("目标数据库库密码:");
        toPasswordLabel.setBounds(10,376,120,25);
        panel.add(toPasswordLabel);
        final JTextField toPasswordField = new JTextField();
        toPasswordField.setBounds(140,376,250,25);
        panel.add(toPasswordField);
        
        JButton toButtonTest = new JButton("test");
        toButtonTest.setBounds(400, 300, 80, 40);
        panel.add(toButtonTest);
       
        toButtonTest.addActionListener(new ActionListener(){
            @Override
            public void actionPerformed(ActionEvent e) {
            	final String toType = toDataBox.getSelectedItem().toString();
            	final String toIp = toIpField.getText();
            	final String toPort = toPortField.getText();
            	final String toDatabase = toDatanameField.getText();
            	final String toUsername = toUsernameField.getText();
            	final String toPassword = toPasswordField.getText();
            	new Thread(new Runnable() {
					@Override
					public void run() {
						try {
							Connection testConnection = testConnection(toType, toDatabase, toPassword, toPort, toIp, toUsername);
							testConnection.close();
							result.append("success connect "+toType+"\r\n");
						} catch (Exception e1) {
							result.append("ERROR "+e1.getMessage()+"\r\n");
						}
					}
				}).start();
            	
            }
        });
        
      //==================to end==========================


        // 创建登录按钮
        JButton loginButton = new JButton("开始转移");
        loginButton.setBounds(200, 700, 100, 30);
        panel.add(loginButton);
        
        // 清空按钮
        JButton clearButton = new JButton("清空日志");
        clearButton.setBounds(30, 700, 100, 30);
        panel.add(clearButton);
        
        clearButton.addActionListener(new ActionListener(){
            @Override
            public void actionPerformed(ActionEvent e) {
            	result.setText("");
            }
        });
        
        loginButton.addActionListener(new ActionListener(){
            @Override
            public void actionPerformed(ActionEvent e) {
            	final String fromType = fromDataBox.getSelectedItem().toString();
            	final String fromIp = fromIpField.getText();
            	final String fromPort = fromPortField.getText();
            	final String fromDatabase = fromDatanameField.getText();
            	final String fromUsername = fromUsernameField.getText();
            	final String fromPassword = fromPasswordField.getText();
            	
            	final String toType = toDataBox.getSelectedItem().toString();
            	final String toIp = toIpField.getText();
            	final String toPort = toPortField.getText();
            	final String toDatabase = toDatanameField.getText();
            	final String toUsername = toUsernameField.getText();
            	final String toPassword = toPasswordField.getText();
            	
            	new Thread(new Runnable() {
					@Override
					public void run() {
						try {
		            		TransferMainHandler.transfer(result,
		            				new DatabaseExeHandler(getDatabaseSource(true,fromType, fromDatabase, fromPassword, fromPort, fromIp, fromUsername)),
		            				new DatabaseExeHandler(getDatabaseSource(false,toType, toDatabase, toPassword, toPort, toIp, toUsername)),
									DatabaseType.valueOf(toType.toUpperCase()));
						} catch (Exception e1) {
							result.append("ERROR "+e1.getMessage()+"\r\n");
						}
					}
				}).start();
            }
        });

    }
    
    private static Connection testConnection(String type,String database,String password,String port,String ip,String username) throws Exception {
    	DatabaseType databaesType = DatabaseType.valueOf(type.toUpperCase());
    	DatabaseProperties properties = new DatabaseProperties();
    	properties.setType(databaesType);
    	properties.buildDatabase(database)
		.buildPassword(password)
		.buildPort(port)
		.buildUrl(ip)
		.buildUsername(username);
    	return DatabaseFactory.getTestConnection(properties);
    }

    private static DataSource getDatabaseSource(boolean origin,String type,String database,String password,String port,String ip,String username) throws SQLException {
    	if(origin) {
    		if(sourceDataSource!=null) {
    			return sourceDataSource;
    		}
    	}else {
    		if(targetDataSource!=null) {
    			return targetDataSource;
    		}
    	}
    	DatabaseType databaesType = DatabaseType.valueOf(type.toUpperCase());
    	DatabaseProperties properties = new DatabaseProperties();
    	properties.setType(databaesType);
    	properties.buildDatabase(database)
		.buildPassword(password)
		.buildPort(port)
		.buildUrl(ip)
		.buildUsername(username);
    	if(origin) {
    		if(sourceDataSource==null) {
    			sourceDataSource = DatabaseFactory.getDataSource(properties);
    			return sourceDataSource;
    		}
    	}else {
    		if(targetDataSource==null) {
    			targetDataSource = DatabaseFactory.getDataSource(properties);
    			return targetDataSource;
    		}
    	}
		return null;
    }
}
