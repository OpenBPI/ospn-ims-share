# ospn-ims-share

ospn-ims-share �ǿ���crossim��IM�������

## ����
ospn-ims-share�ı�������openssl����Ҫ����openssl��so�⡣
 
## ����
**�������ݿ�**
��c3p0-config.xml�ļ�

```xml

<?xml version ="1.0" encoding="UTF-8"?>
<c3p0-config>
    <default-config>
        <property name="driverClass">com.mysql.cj.jdbc.Driver</property>
        <property name="jdbcUrl">jdbc:mysql://[����myaql���ݿ��ַ]/[�������ݿ���]?useSSL=true&amp;verifyServerCertificate=false&amp;serverTimezone=GMT%2B8&amp;allowPublicKeyRetrieval=true&amp;useUnicode=true&amp;characterEncoding=UTF-8&amp;autoReconnect=true</property>
        <property name="user">[�������ݿ��¼�û���]</property>
        <property name="password">[�������ݿ�����]</property>
    </default-config>
</c3p0-config>

```



**���÷���IP��ַ**
��ospn.properties�ļ�

```

ipConnector= ����connector IP��ַ

ipIMServer= ����ims IP��ַ

ipPeer= �����ڽ��ڵ�IP��ַ

```

## ����
���������������ļ�����ͬһĿ¼�¡�

ospn-connector.jar
ospn-ims-share.jar
ospn.properties 
c3p0-config.xml
start.sh

**�򿪷���ǽ�˿�**
8100��8200��8300��8400��8500��8600��8700��8800

����start.sh

## ��֤�����Ƿ����гɹ�

����androidԴ�� https://gitee.com/apowner/crossim-share-android
�޸�IM����IPΪ��ǰIP�����롢ע�Ტ��¼
��ʾ��¼�ɹ�����IM�����ɹ���

## ��֤�Ƿ�����˹�������
����ΪӦ���г����� ����ߴ󰮡�app��ע�Ტ��¼������app֮����Ի��Ӻ��ѣ���ͨ��������빫����������ɹ���

## ����˵��
2021-12-27 �������������ݣ�ospn-ims-share�汾�������û��������ݡ�

