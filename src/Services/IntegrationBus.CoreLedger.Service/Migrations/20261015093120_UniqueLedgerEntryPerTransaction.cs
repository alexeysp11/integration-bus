using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace IntegrationBus.CoreLedger.Service.Migrations
{
    /// <inheritdoc />
    public partial class UniqueLedgerEntryPerTransaction : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            // Collapse duplicates created by redelivered commands before the unique constraint is enforced
            migrationBuilder.Sql("""
                DELETE FROM "LedgerEntries" a
                USING "LedgerEntries" b
                WHERE a."TransactionId" = b."TransactionId" AND a."Id" > b."Id";
                """);

            migrationBuilder.CreateIndex(
                name: "IX_LedgerEntries_TransactionId",
                table: "LedgerEntries",
                column: "TransactionId",
                unique: true);
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropIndex(
                name: "IX_LedgerEntries_TransactionId",
                table: "LedgerEntries");
        }
    }
}
