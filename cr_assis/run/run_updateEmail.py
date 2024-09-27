from cr_assis.connect.updateEmail import UpdateEmail

update = UpdateEmail()
update.save_path = "/mnt/efs/fs1/data_ssh/account_volume/okex"
update.update_account_volume()