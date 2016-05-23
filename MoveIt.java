import java.awt.AWTException;
import java.awt.Robot;


public class MoveIt {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		Runnable r1 = new Runnable() {
			
			@Override
			public void run() {
				
				Robot r = null;
				try {
					r = new Robot();
				
				
					while (true) {
						
						r.mouseMove(100, 200);
						
						Thread.sleep(15000);
						
					}
				
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		};
		
		Thread t = new Thread(r1);
		t.start();
	}

}
