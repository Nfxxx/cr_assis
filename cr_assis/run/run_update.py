from research.utils.updateData import UpdateData
update = UpdateData()
contractsize = update.update_contractsize()
contractsize.to_csv("/home/ssh/parameters/config_buffet/dt/contractsize.csv")