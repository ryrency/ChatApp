package message.utility;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;

public class NetworkUtility {

	public NetworkUtility() {
		// TODO Auto-generated constructor stub
		
		
	}
	
	public static String getLocalHostAddress() {
		String hostAddress = null;
		
		try {
		Enumeration<NetworkInterface> netInterfaceEnum = NetworkInterface.getNetworkInterfaces();
		while(netInterfaceEnum.hasMoreElements()) {
//			System.out.println("***NodeMonitor*** fn:getLocalHostAddress*** Inside while #1*** ");
			NetworkInterface netInterface = netInterfaceEnum.nextElement();
//			System.out.println("NetworkInterface = "+netInterface.toString());
			if(!netInterface.isUp()) {
				continue;
			}
			
			Enumeration<InetAddress> inetAddrEnum = netInterface.getInetAddresses();
			
			while(inetAddrEnum.hasMoreElements()) {
//				System.out.println("***NodeMonitor*** dn:getLocalHostAddress*** Inside while #2");
				InetAddress inetAddr = inetAddrEnum.nextElement();
//				System.out.println("***NodeMonitor*** dn:getLocalHostAddress*** InetAdress = "+inetAddr);
				if(!inetAddr.isLoopbackAddress() && inetAddr instanceof Inet4Address) {
					hostAddress =  inetAddr.getHostAddress().toString();
//					System.out.println("***NodeMonitor*** dn:getLocalHostAddress*** hostAdress = "+hostAddress);
					break;
					
				}
			}
			if(hostAddress != null) {
				break;
			}
		}
	}
	
	catch(Exception e) {
		e.printStackTrace();
		
	}
	finally {
	}
		return hostAddress;

	}
	
	
	 public static String getBroadcastAddress() {
			String broadcastAddress = null;
			
			try {
			Enumeration<NetworkInterface> netInterfaceEnum = NetworkInterface.getNetworkInterfaces();
			while(netInterfaceEnum.hasMoreElements()) {
				System.out.println("***DiscoveryClient*** fn:getBroadcastAddress*** Inside while #1*** ");
				NetworkInterface netInterface = netInterfaceEnum.nextElement();
				System.out.println("NetworkInterface = "+netInterface.toString());
				if(!netInterface.isUp()) {
					continue;
				}
				
				List<InterfaceAddress> inetAddrList = netInterface.getInterfaceAddresses();
				Iterator<InterfaceAddress> iterator = inetAddrList.iterator();
				
				while(iterator.hasNext()) {
					System.out.println("***DiscoveryClient*** dn:getBroadcastAddress*** Inside while #2");
					InterfaceAddress intrAddr = iterator.next();
					System.out.println("***DiscoveryClient*** dn:getBroadcastAddress*** broadcastAdress = "+intrAddr);
					if(intrAddr.getAddress() instanceof Inet4Address) {
						broadcastAddress =  intrAddr.getBroadcast().toString().replace("/", "");
						System.out.println("***DiscoveryClient*** dn:getBroadcastAddress*** broadcastAdress = "+broadcastAddress);
						break;
					}
						
					}
				if(broadcastAddress != null) {
					break;
				}
				}
				
			}
		
		
		catch(Exception e) {
			e.printStackTrace();
			
		}
		finally {
		}
			return broadcastAddress;

		}

}
