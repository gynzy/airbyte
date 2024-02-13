#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#

import logging
from typing import Any, List, Mapping, Tuple

from airbyte_cdk.sources import AbstractSource

from airbyte_cdk.models import AirbyteCatalog
from airbyte_cdk.sources.streams import Stream
from source_exact.streams import (
    CRMAccountClassificationNames,
    CRMAccountClassifications,
    ExactStream,
    SyncCashflowPaymentTerms,
    SyncCRMAccounts,
    SyncCRMAddresses,
    SyncCRMContacts,
    SyncCRMQuotationHeaders,
    SyncCRMQuotationLines,
    SyncDeleted,
    SyncDocumentsDocumentAttachments,
    SyncDocumentsDocuments,
    SyncFinancialGLAccounts,
    SyncFinancialGLClassifications,
    SyncFinancialTransactionLines,
    SyncHRMLeaveAbsenceHoursByDay,
    SyncHRMScheduleEntries,
    SyncHRMSchedules,
    SyncInventoryItemStorageLocations,
    SyncInventoryItemWarehouses,
    SyncInventorySerialBatchNumbers,
    SyncInventoryStockPositions,
    SyncInventoryStockSerialBatchNumbers,
    SyncInventoryStorageLocationStockPositions,
    SyncLogisticsItems,
    SyncLogisticsPurchaseItemPrices,
    SyncLogisticsSalesItemPrices,
    SyncLogisticsSupplierItem,
    SyncManufacturingShopOrderMaterialPlans,
    SyncManufacturingShopOrderRoutingStepPlans,
    SyncManufacturingShopOrders,
    SyncProjectProjectPlanning,
    SyncProjectProjects,
    SyncProjectProjectWBS,
    SyncProjectTimeCostTransactions,
    SyncPurchaseOrderPurchaseOrders,
    SyncSalesSalesPriceListVolumeDiscounts,
    SyncSalesInvoiceSalesInvoices,
    SyncSalesOrderGoodsDeliveries,
    SyncSalesOrderGoodsDeliveryLines,
    SyncSalesOrderSalesOrderHeaders,
    SyncSalesOrderSalesOrderLines,
    SyncSubscriptionSubscriptionLines,
    SyncSubscriptionSubscriptions,
)


# Source
class SourceExact(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        access_token = (config or {}).get("credentials", {}).get("access_token")
        refresh_token = (config or {}).get("credentials", {}).get("refresh_token")
        divisions = (config or {}).get("divisions", [])

        if not access_token or not refresh_token:
            return False, "Missing access or refresh token"
        if not divisions:
            return False, "Missing divisions"

        # TODO: check with airbyte whether control messages are handled during connection check (for token refresh)
        # try:
        #     headers = {
        #         "Authorization": f"Bearer {access_token}",
        #         "Accept": "application/json",
        #     }

        #     response = requests.get(
        #         "https://start.exactonline.nl/api/v1/current/Me",
        #         headers=headers,
        #         timeout=15,
        #     )

        #     response.raise_for_status()
        #     logger.info(f"Connection check successful. Details:\n{json.dumps(response.json())}")
        # except requests.RequestException as exc:
        #     return (
        #         False,
        #         f"Exception happened during connection check. Validate that the access_token is still valid at this point. Details\n{exc}",
        #     )

        return True, None

    def discover(self, logger: logging.Logger, config: Mapping[str, Any]) -> AirbyteCatalog:
        """Implements the Discover operation from the Airbyte Specification.
        See https://docs.airbyte.com/understanding-airbyte/airbyte-protocol/#discover.

        This method filters out any unauthorized streams from the list of all streams this connector supports.
        """

        filtered = []
        for stream in self.streams(config):
            if stream.test_access():
                filtered.append(stream.as_airbyte_stream())
            else:
                logger.info(f"Filtered out following stream: {stream.name}")

        return AirbyteCatalog(streams=filtered)

    def streams(self, config: Mapping[str, Any]) -> List[ExactStream]:
        return [
            CRMAccountClassifications(config),
            CRMAccountClassificationNames(config),
            SyncCashflowPaymentTerms(config),
            SyncCRMAccounts(config),
            SyncCRMAddresses(config),
            SyncCRMContacts(config),
            SyncCRMQuotationHeaders(config),
            SyncCRMQuotationLines(config),
            SyncDeleted(config),
            SyncDocumentsDocumentAttachments(config),
            SyncDocumentsDocuments(config),
            SyncFinancialGLAccounts(config),
            SyncFinancialGLClassifications(config),
            SyncFinancialTransactionLines(config),
            SyncHRMLeaveAbsenceHoursByDay(config),
            SyncHRMScheduleEntries(config),
            SyncHRMSchedules(config),
            SyncInventoryItemStorageLocations(config),
            SyncInventoryItemWarehouses(config),
            SyncInventorySerialBatchNumbers(config),
            SyncInventoryStockPositions(config),
            SyncInventoryStockSerialBatchNumbers(config),
            SyncInventoryStorageLocationStockPositions(config),
            SyncLogisticsItems(config),
            SyncLogisticsPurchaseItemPrices(config),
            SyncLogisticsSalesItemPrices(config),
            SyncLogisticsSupplierItem(config),
            SyncManufacturingShopOrderMaterialPlans(config),
            SyncManufacturingShopOrderRoutingStepPlans(config),
            SyncManufacturingShopOrders(config),
            SyncProjectProjectPlanning(config),
            SyncProjectProjects(config),
            SyncProjectProjectWBS(config),
            SyncProjectTimeCostTransactions(config),
            SyncPurchaseOrderPurchaseOrders(config),
            SyncSalesSalesPriceListVolumeDiscounts(config),
            SyncSalesInvoiceSalesInvoices(config),
            SyncSalesOrderGoodsDeliveries(config),
            SyncSalesOrderGoodsDeliveryLines(config),
            SyncSalesOrderSalesOrderHeaders(config),
            SyncSalesOrderSalesOrderLines(config),
            SyncSubscriptionSubscriptionLines(config),
            SyncSubscriptionSubscriptions(config),
        ]
