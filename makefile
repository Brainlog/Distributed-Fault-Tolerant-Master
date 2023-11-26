bgsave:
	sudo systemctl restart redis-server
	clear
	python3 client.py
raft:
	clear
	python3 client.py
clean:
	rm *.db
	rm *.db.meta
	rm *.db.idx