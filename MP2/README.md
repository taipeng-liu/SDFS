type: 
Introducer /  Non-introducer

Fixed Introducer -> VM01

Introducer: Listener 
				New JOIN 
			Updater 
				Update Membership List
				Update Monitor List	
			Sender 
				if newly-joined nodes
					Send Full Membership List
				else 
					Send JOIN
			Leave/Fail	

Node:		Listener 
				Monitor Hearbeat from 2 predecessor, 1 successor
				Leave/Fail Message
				JOIN from Introducer
			Timer
				Check monitoring nodes' Hearbeat
			Sender
				Create Hearbeat to 2 successor, 1 predecessor
				Create Leave Message
				Leave/Fail Message
				JOIN to introducer
			Updater:
				Update Membership List
				Update Monitor List

Message:
			JOIN (First join system; Introducer relay)
			Hearbeat (Send to monitor list)	
			Leave (Send to monitor when leave; Monitors relay)
			Fail (Monitors detect then relay)
