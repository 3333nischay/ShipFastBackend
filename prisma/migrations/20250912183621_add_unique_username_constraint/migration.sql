/*
  Warnings:

  - A unique constraint covering the columns `[username]` on the table `UserDB` will be added. If there are existing duplicate values, this will fail.

*/
-- CreateIndex
CREATE UNIQUE INDEX "UserDB_username_key" ON "public"."UserDB"("username");
