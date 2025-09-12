-- CreateTable
CREATE TABLE "public"."TopSlowQuery" (
    "id" TEXT NOT NULL,
    "userDbId" TEXT NOT NULL,
    "userId" TEXT NOT NULL,
    "query" TEXT NOT NULL,
    "calls" INTEGER NOT NULL DEFAULT 1,
    "totalTimeMs" DOUBLE PRECISION NOT NULL,
    "meanTimeMs" DOUBLE PRECISION NOT NULL,
    "rowsReturned" INTEGER NOT NULL,
    "rank" INTEGER NOT NULL,
    "collectedAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "TopSlowQuery_pkey" PRIMARY KEY ("id")
);
