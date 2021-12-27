# ospn-ims-share

ospn-ims-share 是跨云crossim的IM共享服务。

## 编译
ospn-ims-share的编译依赖openssl，需要编译openssl的so库。
 
## 配置
**配置数据库**
打开c3p0-config.xml文件

```xml

<?xml version ="1.0" encoding="UTF-8"?>
<c3p0-config>
    <default-config>
        <property name="driverClass">com.mysql.cj.jdbc.Driver</property>
        <property name="jdbcUrl">jdbc:mysql://[配置myaql数据库地址]/[配置数据库名]?useSSL=true&amp;verifyServerCertificate=false&amp;serverTimezone=GMT%2B8&amp;allowPublicKeyRetrieval=true&amp;useUnicode=true&amp;characterEncoding=UTF-8&amp;autoReconnect=true</property>
        <property name="user">[配置数据库登录用户名]</property>
        <property name="password">[配置数据库密码]</property>
    </default-config>
</c3p0-config>

```



**配置服务IP地址**
打开ospn.properties文件

```

ipConnector= 配置connector IP地址

ipIMServer= 配置ims IP地址

ipPeer= 配置邻近节点IP地址

```

## 部署
将编译生成以下文件放在同一目录下。

ospn-connector.jar
ospn-ims-share.jar
ospn.properties 
c3p0-config.xml
start.sh

**打开防火墙端口**
8100、8200、8300、8400、8500、8600、8700、8800

运行start.sh

## 验证服务是否运行成功

下载android源码 https://gitee.com/apowner/crossim-share-android
修改IM链接IP为当前IP，编译、注册并登录
提示登录成功则共享IM服务搭建成功。

## 验证是否加入了公共网络
到华为应用市场下载 【身边大爱】app，注册并登录，两款app之间可以互加好友，互通聊天则加入公共聊天网络成功。

## 更新说明
2021-12-27 分离了敏感数据，ospn-ims-share版本不保留用户敏感数据。

