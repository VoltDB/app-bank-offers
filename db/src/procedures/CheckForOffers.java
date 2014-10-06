package procedures;

//import procedures.PerformanceTimer;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.client.ClientResponse;
import org.voltdb.types.TimestampType;
import java.util.*;


public class CheckForOffers extends VoltProcedure {


	public final SQLStmt insertTxn = new SQLStmt(
		"INSERT INTO TRANSACTION VALUES ( ?,?,?,?,?,?,?);");

    public final SQLStmt checkCustState = new SQLStmt(
        "SELECT c.cust_state "+
        "FROM account a "+
        "INNER JOIN customer c ON a.cust_id = c.cust_id "+
        "WHERE a.acc_no = ?;");


	// check for vendor offers
	public final SQLStmt getVendorOffers = new SQLStmt(
		"SELECT vo.offer_text "+
		"FROM acct_vendor_totals avt "+
		"INNER JOIN vendor_offers vo ON avt.vendor_id = vo.vendor_id "+
		"WHERE "+
		"  avt.acc_no = ? AND"+
		"  avt.vendor_id = ? AND"+
		"  avt.total_visits > vo.min_visits AND"+
		"  avt.total_spend > vo.min_spend "+
		"ORDER BY vo.offer_priority, vo.offer_text;");

	public final SQLStmt insertOffer = new SQLStmt(
		"INSERT INTO offers_given VALUES (?,?,NOW,?);");

	// public final SQLStmt insertOfferExport = new SQLStmt(
	// 	"INSERT INTO offers_given_exp VALUES (?,?,NOW,?);");



	public long run( long txnId,
	                 long acctNo,
	                 double txnAmt,
	                 String txnState,
	                 String txnCity,
	                 TimestampType txnTimestamp,
	                 int vendorId)
		throws VoltAbortException {

		
		// insert transaction
		voltQueueSQL(insertTxn,txnId,acctNo,txnAmt,txnState,txnCity,txnTimestamp,vendorId);

		// get vendor offers
		voltQueueSQL(getVendorOffers,acctNo,vendorId);
		
        VoltTable[] results0 = voltExecuteSQL();

        if (results0[1].getRowCount() > 0) { // offers found
	        results0[1].advanceRow();
	        String offerText = results0[1].getString(0);
	        voltQueueSQL(insertOffer,acctNo,vendorId,offerText);
	        //voltQueueSQL(insertOfferExport,acctNo,vendorId,offerText);
	        voltExecuteSQL();
        } 
        
        return ClientResponse.SUCCESS;
    }
}
