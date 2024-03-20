## from blitzmodels

# @pytest.mark.asyncio
# @TANKS_JSON_FILES
# async def test_8_Tank_WGTank_transformation(datafiles: Path) -> None:
#     tanks_json = TanksJsonList()
#     for tanks_json_fn in datafiles.iterdir():
#         async with aiofiles.open(tanks_json_fn) as file:
#             try:
#                 tanks_json = TanksJsonList.parse_raw(await file.read())
#             except Exception as err:
#                 assert False, f"Parsing test file List[BSTank] failed: {basename(tanks_json_fn)}"
#     for tank in tanks_json:
#         if (wgtank := Tank.transform(tank)) is None:
#             assert False, f"could transform BSTank to Tank: tank_id={tank.tank_id} {tank}"
#         assert (
#             wgtank.tank_id == tank.tank_id
#         ), f"tank_id does not match after Tank.transform(BSTank): tank_id={tank.tank_id} {tank}"
#         assert (
#             wgtank.name == tank.name
#         ), f"name does not match after Tank.transform(BSTank): tank_id={tank.tank_id} {tank}"
#         assert (
#             wgtank.type is not None and tank.type is not None
#         ), f"could not transform tank type: tank_id={tank.tank_id} {tank}"
#         assert (
#             wgtank.type.name == tank.type.name
#         ), f"tank type does not match after Tank.transform(BSTank): tank_id={tank.tank_id} {tank}"
